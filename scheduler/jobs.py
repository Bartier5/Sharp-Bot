import logging
import aiohttp
from datetime import datetime, timedelta
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from telegram.ext import Application

from config import (
    MORNING_BRIEFING_HOUR,
    ODDS_REFRESH_MINUTES,
    RESULT_CHECKER_HOURS,
    LEAGUE_MODE_MAP,
    MODE_ORIGINAL,
    MODE_HVLO,
    MODE_NBA,
    ORIGINAL_LEAGUES,
    HVLO_LEAGUES,
    GRADE_A, GRADE_B, GRADE_C,
)
from data.storage import (
    get_all_users,
    get_todays_fixtures,
    get_unsettled_fixtures,
    get_todays_picks,
    get_bets_for_pick,
    upsert_fixture,
    insert_pick,
    update_pick_odds,
    update_fixture_result,
    settle_pick,
    settle_bet,
)
from data.football_fetcher import (
    make_session,
    get_todays_fixtures as fetch_football_fixtures,
    get_standings,
    get_team_stats,
    get_last_fixtures,
    get_win_by_2_rate,
    get_injuries_by_team,
    get_league_avg_goals,
    get_fixture_result,
    _current_season,
)
from data.nba_fetcher import (
    get_todays_nba_fixtures,
    get_nba_standings,
    get_nba_team_stats,
    get_last_nba_games,
    get_nba_player_status,
    get_top_players_by_minutes,
    get_nba_offense_rankings,
    get_nba_game_result,
    calculate_losing_streak,
)
from data.odds_fetcher import (
    get_odds_for_picks,
    store_opening_odds,
)
from data.football_filter import (
    evaluate_fixture,
    settle_market,
)
from data.nba_filter import (
    evaluate_nba_fixture,
    settle_nba_market,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# SCHEDULER FACTORY
# ─────────────────────────────────────────

def create_scheduler(app: Application) -> AsyncIOScheduler:
    """
    Create and configure the APScheduler instance.
    All three jobs are registered here.
    Returns the scheduler — main.py calls .start() on it.
    """
    scheduler = AsyncIOScheduler(timezone="UTC")

    # Job 1 — Morning briefing at 8AM UTC daily
    scheduler.add_job(
        func=lambda: _run_morning_briefing_job(app),
        trigger=CronTrigger(hour=MORNING_BRIEFING_HOUR,
                            minute=0, timezone="UTC"),
        id="morning_briefing",
        name="Morning Briefing",
        replace_existing=True,
    )

    # Job 2 — Odds refresh every 30 minutes
    scheduler.add_job(
        func=lambda: _run_odds_refresh_job(app),
        trigger=IntervalTrigger(minutes=ODDS_REFRESH_MINUTES),
        id="odds_refresh",
        name="Odds Refresh",
        replace_existing=True,
    )

    # Job 3 — Result checker every 2 hours
    scheduler.add_job(
        func=lambda: _run_result_checker_job(app),
        trigger=IntervalTrigger(hours=RESULT_CHECKER_HOURS),
        id="result_checker",
        name="Result Checker",
        replace_existing=True,
    )

    logger.info("Scheduler configured — 3 jobs registered")
    return scheduler


# ─────────────────────────────────────────
# PUBLIC INTERFACES
# Called by handlers.py for manual triggers
# ─────────────────────────────────────────

async def run_morning_briefing(app: Application,
                                user_id: int = None) -> None:
    """
    Public interface for manual briefing trigger.
    Called by /briefing command in handlers.py.
    user_id: if provided, sends only to that user.
             if None, sends to all registered users.
    """
    await _run_morning_briefing_job(app, target_user=user_id)


async def run_result_checker(app: Application) -> None:
    """
    Public interface for manual settle trigger.
    Called by confirm_settle callback in handlers.py.
    """
    await _run_result_checker_job(app)


# ─────────────────────────────────────────
# JOB 1 — MORNING BRIEFING
# ─────────────────────────────────────────

async def _run_morning_briefing_job(
        app: Application,
        target_user: int = None) -> None:
    """
    Fetches today's fixtures, runs SHARP filters,
    stores qualified picks, and sends briefing messages.

    Steps:
    1. Fetch all fixtures for today
    2. For each fixture run the appropriate filter
    3. Store qualified picks in DB
    4. Fetch odds for qualified picks
    5. Send personalized briefing to each user
    """
    logger.info("Morning briefing job started")
    start_time = datetime.utcnow()

    async with make_session() as session:

        # ── Step 1: Fetch football fixtures ──────────────────────
        football_fixtures = await fetch_football_fixtures(session)
        nba_fixtures      = await get_todays_nba_fixtures(session)
        all_fixtures      = football_fixtures + nba_fixtures

        logger.info(
            f"Fetched {len(football_fixtures)} football + "
            f"{len(nba_fixtures)} NBA fixtures"
        )

        # Store all fixtures
        for fix in all_fixtures:
            await upsert_fixture(fix)

        # ── Step 2 & 3: Filter and store picks ───────────────────
        all_picks = []

        for fix in football_fixtures:
            picks = await _filter_football_fixture(
                session, fix
            )
            all_picks.extend(picks)

        for fix in nba_fixtures:
            picks = await _filter_nba_fixture(session, fix)
            all_picks.extend(picks)

        logger.info(
            f"Filter complete — {len(all_picks)} picks qualified"
        )

        # ── Step 4: Fetch odds for qualified picks ────────────────
        if all_picks:
            odds_map = await get_odds_for_picks(
                session, all_picks
            )
            for pick in all_picks:
                fid      = pick.get("fixture_id")
                odds_obj = odds_map.get(fid)
                if odds_obj:
                    pick["opening_odds"] = store_opening_odds(
                        odds_obj
                    )

        # ── Step 5: Send briefings ────────────────────────────────
        if target_user:
            users = [{"user_id": target_user,
                      "sport_preference": "both"}]
        else:
            users = await get_all_users()

        for user in users:
            await _send_briefing(
                app, user, all_picks
            )

    elapsed = (datetime.utcnow() - start_time).seconds
    logger.info(
        f"Morning briefing complete in {elapsed}s — "
        f"{len(all_picks)} picks sent"
    )


async def _filter_football_fixture(
        session: aiohttp.ClientSession,
        fixture: dict) -> list[dict]:
    """
    Fetch all required data for one football fixture
    and run it through the SHARP filter engine.
    Returns list of pick dicts ready for DB insertion.
    """
    fid        = fixture["fixture_id"]
    league_id  = fixture["league_id"]
    home_id    = fixture.get("home_id")
    away_id    = fixture.get("away_id")

    try:
        # Fetch standings for position check
        standings = await get_standings(session, league_id)
        if not standings:
            logger.warning(
                f"[{fid}] No standings — skipping"
            )
            return []

        # Fetch team stats
        home_stats = await get_team_stats(
            session, home_id, league_id
        )
        away_stats = await get_team_stats(
            session, away_id, league_id
        )

        if not home_stats or not away_stats:
            logger.warning(
                f"[{fid}] Missing team stats — skipping"
            )
            return []

        # Fetch form (last 5 home/away)
        home_form = await get_last_fixtures(
            session, home_id, count=5, venue="home"
        ) or []
        away_form = await get_last_fixtures(
            session, away_id, count=5, venue="away"
        ) or []

        # Fetch injuries for home team
        injuries = await get_injuries_by_team(
            session, home_id, int(fid)
        )

        # League avg goals for U1.5 filter
        league_avg = await get_league_avg_goals(
            session, league_id
        )

        # Win by 2+ rate for AH -1.5 filter
        win_by_2 = await get_win_by_2_rate(
            session, home_id, league_id
        )

        # Run filter engine
        result = evaluate_fixture(
            fixture=fixture,
            home_stats=home_stats,
            away_stats=away_stats,
            standings=standings,
            home_form=home_form,
            away_form=away_form,
            injuries=injuries,
            league_avg=league_avg,
            win_by_2_rate=win_by_2,
            referee_cards=None,
            odds_data=None,
        )

        if not result.passed:
            logger.info(
                f"[{fid}] Filtered out — {result.void_reason}"
            )
            return []

        # Convert qualified markets to pick dicts
        picks = []
        for market in result.qualified_markets:
            pick_id = await insert_pick({
                "fixture_id":      fid,
                "sport":           "football",
                "mode":            fixture["mode"],
                "market":          market.market,
                "qualifier_flags": market.qualifier_flags,
                "confidence":      market.confidence,
                "odds":            market.odds,
                "opening_odds":    market.opening_odds,
            })
            picks.append({
                "id":          pick_id,
                "fixture_id":  fid,
                "sport":       "football",
                "mode":        fixture["mode"],
                "market":      market.market,
                "confidence":  market.confidence,
                "odds":        market.odds,
                "home_team":   fixture["home_team"],
                "away_team":   fixture["away_team"],
                "kickoff":     fixture["kickoff"],
                "league_id":   league_id,
            })

        return picks

    except Exception as e:
        logger.error(
            f"[{fid}] Football filter error: {e}"
        )
        return []


async def _filter_nba_fixture(
        session: aiohttp.ClientSession,
        fixture: dict) -> list[dict]:
    """
    Fetch all required data for one NBA fixture
    and run it through the SHARP NBA filter engine.
    Returns list of pick dicts ready for DB insertion.
    """
    fid     = fixture["fixture_id"]
    home_id = fixture.get("home_id")
    away_id = fixture.get("away_id")

    try:
        # Fetch standings for win pct
        standings = await get_nba_standings(session)
        if not standings:
            return []

        from data.nba_fetcher import get_team_record
        home_record = get_team_record(standings, home_id) or {}
        away_record = get_team_record(standings, away_id) or {}

        # Team stats
        home_stats = await get_nba_team_stats(
            session, home_id
        ) or {}
        away_stats = await get_nba_team_stats(
            session, away_id
        ) or {}

        # Form — last 5 games
        home_form = await get_last_nba_games(
            session, home_id, count=5
        ) or []
        away_form = await get_last_nba_games(
            session, away_id, count=5
        ) or []

        # Player status
        home_injuries = await get_nba_player_status(
            session, home_id, int(fid)
        )
        away_injuries = await get_nba_player_status(
            session, away_id, int(fid)
        )

        # Top players by minutes
        home_top = await get_top_players_by_minutes(
            session, home_id
        )
        away_top = await get_top_players_by_minutes(
            session, away_id
        )

        # Offense rankings
        rankings = await get_nba_offense_rankings(session)

        # Run NBA filter
        result = evaluate_nba_fixture(
            fixture=fixture,
            home_stats=home_stats,
            away_stats=away_stats,
            home_record=home_record,
            away_record=away_record,
            home_form=home_form,
            away_form=away_form,
            home_injuries=home_injuries,
            away_injuries=away_injuries,
            home_top_players=home_top,
            away_top_players=away_top,
            offense_rankings=rankings,
            odds_data=None,
        )

        if not result.passed:
            logger.info(
                f"[{fid}] NBA filtered — {result.void_reason}"
            )
            return []

        picks = []
        for market in result.qualified_markets:
            pick_id = await insert_pick({
                "fixture_id":      fid,
                "sport":           "nba",
                "mode":            MODE_NBA,
                "market":          market.market,
                "qualifier_flags": market.qualifier_flags,
                "confidence":      market.confidence,
                "odds":            market.odds,
                "opening_odds":    market.opening_odds,
            })
            picks.append({
                "id":         pick_id,
                "fixture_id": fid,
                "sport":      "nba",
                "mode":       MODE_NBA,
                "market":     market.market,
                "confidence": market.confidence,
                "odds":       market.odds,
                "home_team":  fixture["home_team"],
                "away_team":  fixture["away_team"],
                "kickoff":    fixture["kickoff"],
                "league_id":  12,
            })

        return picks

    except Exception as e:
        logger.error(f"[{fid}] NBA filter error: {e}")
        return []


async def _send_briefing(
        app: Application,
        user: dict,
        all_picks: list[dict]) -> None:
    """
    Send morning briefing message to a single user.
    Filters picks by user's sport preference.
    """
    user_id = user["user_id"]
    pref    = user.get("sport_preference", "both")

    # Filter picks by preference
    if pref == "football":
        picks = [p for p in all_picks
                 if p["sport"] == "football"]
    elif pref == "nba":
        picks = [p for p in all_picks
                 if p["sport"] == "nba"]
    else:
        picks = all_picks

    now      = datetime.utcnow().strftime("%A %d %B %Y")
    grade_a  = sum(
        1 for p in picks if p.get("confidence") == GRADE_A
    )
    grade_b  = sum(
        1 for p in picks if p.get("confidence") == GRADE_B
    )

    if not picks:
        text = (
            f"🌅 <b>Morning Briefing — {now}</b>\n\n"
            f"📭 No qualified picks today.\n\n"
            f"The model found no fixtures passing all "
            f"SHARP v3 filters. Check back tomorrow."
        )
    else:
        lines = [
            f"🌅 <b>Morning Briefing — {now}</b>\n",
            f"🟢 Grade A: {grade_a}  🟡 Grade B: {grade_b}\n",
        ]

        for pick in picks[:10]:    # cap at 10 per briefing
            sport_e  = "⚽" if pick["sport"] == "football" \
                else "🏀"
            grade_e  = {"A": "🟢", "B": "🟡",
                        "C": "🟠"}.get(
                pick.get("confidence", ""), "⚪"
            )
            market   = pick["market"].replace(
                "_", " "
            ).upper()
            odds_str = (
                f"@ {pick['odds']:.2f}"
                if pick.get("odds") else ""
            )
            kickoff  = str(
                pick.get("kickoff", "")
            )[:16]

            lines.append(
                f"{sport_e} {grade_e} "
                f"<b>{pick['home_team']} vs "
                f"{pick['away_team']}</b>\n"
                f"   {market} {odds_str} | ⏰ {kickoff}\n"
                f"   /pick {pick['id']}\n"
            )

        if len(picks) > 10:
            lines.append(
                f"\n<i>+{len(picks) - 10} more picks. "
                f"Use /today to see all.</i>"
            )

        text = "\n".join(lines)

    try:
        await app.bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode="HTML",
        )
        logger.info(f"Briefing sent to user {user_id}")
    except Exception as e:
        logger.warning(
            f"Could not send briefing to {user_id}: {e}"
        )


# ─────────────────────────────────────────
# JOB 2 — ODDS REFRESH
# ─────────────────────────────────────────

async def _run_odds_refresh_job(app: Application) -> None:
    """
    Refresh odds for picks kicking off within 3 hours.
    Updates current odds in the picks table.
    Only runs within the pre-kickoff window to save quota.
    """
    logger.info("Odds refresh job started")
    now         = datetime.utcnow()
    window_end  = now + timedelta(hours=3)

    todays_picks = await get_todays_picks()
    if not todays_picks:
        return

    # Filter to picks kicking off within 3 hours
    upcoming = []
    for pick in todays_picks:
        try:
            kickoff = datetime.fromisoformat(
                str(pick.get("kickoff", ""))
                .replace("Z", "+00:00")
            ).replace(tzinfo=None)
            if now <= kickoff <= window_end:
                upcoming.append(pick)
        except Exception:
            continue

    if not upcoming:
        logger.info("No picks in 3-hour window — skipping odds refresh")
        return

    logger.info(
        f"Refreshing odds for {len(upcoming)} upcoming picks"
    )

    async with make_session() as session:
        odds_map = await get_odds_for_picks(session, upcoming)

        for pick in upcoming:
            fid      = pick["fixture_id"]
            odds_obj = odds_map.get(fid)
            if not odds_obj:
                continue

            # Extract relevant odds for this market
            from data.football_filter import _get_market_odds \
                as _get_football_odds
            new_odds = None

            try:
                markets = odds_obj.get("markets", {})
                h2h     = markets.get("h2h", {})
                home    = odds_obj.get("home_team", "")

                if pick["market"] in (
                    "ml_home", "nba_ml_home",
                    "nba_parlay_leg", "nba_1h_ml"
                ):
                    new_odds = h2h.get(home)
                elif pick["market"] in (
                    "ml_away", "nba_ml_away"
                ):
                    away     = odds_obj.get("away_team", "")
                    new_odds = h2h.get(away)
                else:
                    from data.odds_fetcher import get_over_odds
                    if "over_1.5" in pick["market"]:
                        new_odds = get_over_odds(
                            odds_obj, 1.5
                        )
                    elif "over_2.5" in pick["market"]:
                        new_odds = get_over_odds(
                            odds_obj, 2.5
                        )

                if new_odds:
                    await update_pick_odds(
                        pick["id"], new_odds
                    )
                    logger.info(
                        f"Pick {pick['id']} odds updated "
                        f"→ {new_odds}"
                    )

            except Exception as e:
                logger.error(
                    f"Odds update error for pick "
                    f"{pick['id']}: {e}"
                )


# ─────────────────────────────────────────
# JOB 3 — RESULT CHECKER
# ─────────────────────────────────────────

async def _run_result_checker_job(app: Application) -> None:
    """
    Check finished fixtures and settle open bets.

    Steps:
    1. Fetch all unsettled fixtures from DB
    2. Check current status via API
    3. For finished fixtures — settle all linked picks
    4. For each settled pick — settle all linked bets
    5. Notify users of settled bets
    """
    logger.info("Result checker job started")

    unsettled = await get_unsettled_fixtures()
    if not unsettled:
        logger.info("No unsettled fixtures — result checker done")
        return

    logger.info(
        f"Checking {len(unsettled)} unsettled fixtures"
    )

    async with make_session() as session:
        for fixture in unsettled:
            fid   = fixture["fixture_id"]
            sport = fixture["sport"]

            try:
                # Fetch current result
                if sport == "nba":
                    result = await get_nba_game_result(
                        session, int(fid)
                    )
                else:
                    result = await get_fixture_result(
                        session, int(fid)
                    )

                if not result or not result.get("finished"):
                    continue

                home_score = result["home_score"] or 0
                away_score = result["away_score"] or 0

                # Update fixture in DB
                await update_fixture_result(
                    fid,
                    home_score,
                    away_score,
                    "finished",
                )

                logger.info(
                    f"[{fid}] Final: "
                    f"{fixture['home_team']} {home_score} – "
                    f"{away_score} {fixture['away_team']}"
                )

                # Settle all picks for this fixture
                picks = await get_todays_picks()
                fixture_picks = [
                    p for p in picks
                    if p["fixture_id"] == fid
                ]

                for pick in fixture_picks:
                    market = pick["market"]

                    # Determine result
                    if sport == "nba":
                        outcome = settle_nba_market(
                            market, home_score, away_score
                        )
                    else:
                        outcome = settle_market(
                            market, home_score, away_score
                        )

                    await settle_pick(pick["id"], outcome)

                    # Settle all bets on this pick
                    open_bets = await get_bets_for_pick(
                        pick["id"]
                    )
                    for bet in open_bets:
                        actual_return = (
                            bet["stake"] * bet["odds"]
                            if outcome == "won"
                            else 0.0
                        )
                        await settle_bet(
                            bet["id"],
                            outcome,
                            actual_return,
                        )

                        # Notify the user
                        await _notify_bet_result(
                            app,
                            bet,
                            pick,
                            fixture,
                            outcome,
                            actual_return,
                        )

            except Exception as e:
                logger.error(
                    f"Result checker error for [{fid}]: {e}"
                )
                continue

    logger.info("Result checker job complete")


async def _notify_bet_result(
        app: Application,
        bet: dict,
        pick: dict,
        fixture: dict,
        outcome: str,
        actual_return: float) -> None:
    """
    Send a push notification to a user when their bet settles.
    """
    user_id  = bet["user_id"]
    stake    = bet["stake"]
    odds_val = bet["odds"]
    market   = pick["market"].replace("_", " ").upper()
    home     = fixture["home_team"]
    away     = fixture["away_team"]
    home_s   = fixture.get("home_score", 0)
    away_s   = fixture.get("away_score", 0)

    if outcome == "won":
        profit = actual_return - stake
        emoji  = "✅"
        result_line = (
            f"Profit: <b>+£{profit:.2f}</b> "
            f"(returned £{actual_return:.2f})"
        )
    elif outcome == "lost":
        emoji       = "❌"
        result_line = f"Lost: <b>-£{stake:.2f}</b>"
    else:
        emoji       = "↩️"
        result_line = "Bet voided — stake returned"

    text = (
        f"{emoji} <b>Bet Settled</b>\n\n"
        f"⚽ <b>{home} {home_s} – {away_s} {away}</b>\n"
        f"📌 {market}\n"
        f"Stake: £{stake:.2f} @ {odds_val:.2f}\n"
        f"{result_line}\n\n"
        f"Use /performance to see your full stats."
    )

    try:
        await app.bot.send_message(
            chat_id=user_id,
            text=text,
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning(
            f"Could not notify user {user_id}: {e}"
        )