import logging
import pandas as pd
from datetime import datetime
from telegram import Update
from telegram.ext import CallbackContext
from telegram.constants import ParseMode

from config import (
    MODE_ORIGINAL, MODE_HVLO, MODE_NBA,
    ORIGINAL_LEAGUES, HVLO_LEAGUES,
    GRADE_A, GRADE_B, GRADE_C,
)
from data.storage import (
    upsert_user, get_user,
    update_sport_preference, update_min_games,
    get_todays_picks, get_pick,
    insert_bet, get_open_bets,
    get_bet_history, get_performance_stats,
    get_settled_picks, get_todays_fixtures,
)
from data.odds_fetcher import (
    get_fixture_odds, odds_to_implied_prob,
)
from bot.keyboards import (
    main_menu_keyboard, sport_preference_keyboard,
    settings_keyboard, min_games_keyboard,
    picks_keyboard, pick_action_keyboard,
    bet_confirm_keyboard, open_bets_keyboard,
    backtest_keyboard, settle_confirm_keyboard,
    leagues_keyboard, back_to_menu,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

async def _reply(update: Update, text: str,
                 keyboard=None, parse_mode=ParseMode.HTML) -> None:
    """
    Unified reply function.
    Handles both message replies and callback query edits.
    """
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=keyboard,
            parse_mode=parse_mode,
        )
    elif update.message:
        await update.message.reply_text(
            text=text,
            reply_markup=keyboard,
            parse_mode=parse_mode,
        )


def _grade_emoji(grade: str) -> str:
    """Map confidence grade to emoji for display."""
    return {"A": "🟢", "B": "🟡", "C": "🟠"}.get(grade, "⚪")


def _sport_emoji(sport: str) -> str:
    return {"football": "⚽", "nba": "🏀"}.get(sport, "🎯")


def _format_pick(pick: dict, fixture: dict = None) -> str:
    """Format a single pick into a readable Telegram message."""
    grade   = pick.get("confidence", "?")
    emoji   = _grade_emoji(grade)
    market  = pick.get("market", "").replace("_", " ").upper()
    sport   = pick.get("sport", "football")
    s_emoji = _sport_emoji(sport)

    home = pick.get("home_team", "Home")
    away = pick.get("away_team", "Away")

    # Kickoff time formatting
    kickoff = pick.get("kickoff", "")
    try:
        dt      = datetime.fromisoformat(
            str(kickoff).replace("Z", "+00:00")
        )
        kickoff = dt.strftime("%H:%M UTC")
    except Exception:
        kickoff = str(kickoff)[:16]

    odds_str = (
        f"@ <b>{pick['odds']:.2f}</b>"
        if pick.get("odds") else "odds TBC"
    )

    flags     = pick.get("qualifier_flags", {})
    flag_text = "\n".join(
        f"  {'✅' if v else '❌'} {k.replace('_', ' ')}: "
        f"<code>{v}</code>"
        for k, v in flags.items()
        if isinstance(v, bool)
    )

    return (
        f"{s_emoji} <b>{home} vs {away}</b>\n"
        f"⏰ {kickoff}\n"
        f"📌 Market: <b>{market}</b>\n"
        f"{emoji} Grade: <b>{grade}</b> {odds_str}\n"
        f"\n<i>Filter flags:</i>\n{flag_text}\n"
        f"🆔 Pick ID: <code>{pick.get('id', '?')}</code>"
    )


# ─────────────────────────────────────────
# /start
# ─────────────────────────────────────────

async def start(update: Update,
                context: CallbackContext) -> None:
    """Register user and show welcome message + main menu."""
    user = update.effective_user
    await upsert_user(user.id, user.username or user.first_name)

    text = (
        f"👋 Welcome to <b>SHARP Bot</b>, {user.first_name}!\n\n"
        f"Your personal SHARP Model v3 analyst — football "
        f"and NBA picks filtered through a strict 4-stage "
        f"model.\n\n"
        f"<b>Three modes:</b>\n"
        f"⚽ Original — small leagues, strict filters\n"
        f"⚡ HVLO — youth/reserve accumulator picks\n"
        f"🏀 NBA — moneyline, spreads, team totals\n\n"
        f"Use the menu below or type /help for all commands."
    )
    await _reply(update, text, main_menu_keyboard())


# ─────────────────────────────────────────
# /help
# ─────────────────────────────────────────

async def help_command(update: Update,
                        context: CallbackContext) -> None:
    """Full command reference."""
    text = (
        "📖 <b>SHARP Bot — Command Reference</b>\n\n"
        "<b>Picks</b>\n"
        "/today — today's qualified picks\n"
        "/fixtures — raw fixtures before filtering\n"
        "/pick [id] — deep dive on a single pick\n"
        "/odds [id] — live odds for a fixture\n\n"
        "<b>Betting</b>\n"
        "/bet [pick_id] [stake] — log a bet\n"
        "/bets — your open bets\n"
        "/settle — trigger result check\n"
        "/journal — full bet history\n"
        "/performance — ROI and win rate stats\n\n"
        "<b>Analysis</b>\n"
        "/backtest [sport] [days] — model hit rate\n"
        "/model — current SHARP v3 thresholds\n"
        "/leagues — monitored leagues\n\n"
        "<b>Settings</b>\n"
        "/setpreference — change sport preference\n"
        "/setgames [8-30] — min games threshold\n"
        "/briefing — trigger morning briefing now\n"
    )
    await _reply(update, text, back_to_menu())


# ─────────────────────────────────────────
# /today
# ─────────────────────────────────────────

async def today(update: Update,
                context: CallbackContext) -> None:
    """Show today's SHARP qualified picks."""
    user    = update.effective_user
    db_user = await get_user(user.id)
    pref    = db_user.get("sport_preference", "both") \
        if db_user else "both"

    # Fetch picks based on preference
    if pref == "football":
        picks = await get_todays_picks(sport="football")
    elif pref == "nba":
        picks = await get_todays_picks(sport="nba")
    else:
        picks = await get_todays_picks()

    if not picks:
        await _reply(
            update,
            "📭 No qualified picks for today yet.\n\n"
            "The morning briefing runs at 8AM UTC. "
            "You can also trigger it manually with /briefing",
            back_to_menu(),
        )
        return

    # Summary header
    grade_a = sum(1 for p in picks if p["confidence"] == GRADE_A)
    grade_b = sum(1 for p in picks if p["confidence"] == GRADE_B)
    grade_c = sum(1 for p in picks if p["confidence"] == GRADE_C)

    text = (
        f"📋 <b>Today's SHARP Picks</b>\n\n"
        f"🟢 Grade A: {grade_a}  "
        f"🟡 Grade B: {grade_b}  "
        f"🟠 Grade C: {grade_c}\n\n"
        f"Tap a pick for details and to log a bet:"
    )
    await _reply(update, text, picks_keyboard(picks))


# ─────────────────────────────────────────
# /fixtures
# ─────────────────────────────────────────

async def fixtures(update: Update,
                   context: CallbackContext) -> None:
    """Show raw today's fixtures before SHARP filtering."""
    all_fixtures = await get_todays_fixtures()

    if not all_fixtures:
        await _reply(
            update,
            "📭 No fixtures found for today.",
            back_to_menu(),
        )
        return

    lines = ["📅 <b>Today's Fixtures</b>\n"]
    for f in all_fixtures:
        kickoff = str(f.get("kickoff", ""))[:16]
        status  = f.get("status", "NS")
        lines.append(
            f"{_sport_emoji(f['sport'])} "
            f"<b>{f['home_team']}</b> vs "
            f"<b>{f['away_team']}</b>\n"
            f"   ⏰ {kickoff} | 🏆 {f['league']} "
            f"| {status}\n"
        )

    await _reply(update, "\n".join(lines), back_to_menu())


# ─────────────────────────────────────────
# /pick [id]
# ─────────────────────────────────────────

async def pick(update: Update,
               context: CallbackContext) -> None:
    """Deep dive on a single pick by ID."""
    if not context.args:
        await _reply(
            update,
            "Usage: /pick [id]\nExample: /pick 7",
            back_to_menu(),
        )
        return

    try:
        pick_id = int(context.args[0])
    except ValueError:
        await _reply(update, "❌ Pick ID must be a number.")
        return

    pick_data = await get_pick(pick_id)
    if not pick_data:
        await _reply(
            update,
            f"❌ Pick {pick_id} not found.",
            back_to_menu(),
        )
        return

    text = _format_pick(pick_data)
    await _reply(update, text, pick_action_keyboard(pick_id))


# ─────────────────────────────────────────
# /odds [fixture_id]
# ─────────────────────────────────────────

async def odds(update: Update,
               context: CallbackContext) -> None:
    """Fetch and display live odds for a fixture."""
    if not context.args:
        await _reply(
            update,
            "Usage: /odds [pick_id]\nExample: /odds 7",
            back_to_menu(),
        )
        return

    try:
        pick_id = int(context.args[0])
    except ValueError:
        await _reply(update, "❌ Pick ID must be a number.")
        return

    pick_data = await get_pick(pick_id)
    if not pick_data:
        await _reply(update, f"❌ Pick {pick_id} not found.")
        return

    await _reply(update, "🔄 Fetching live odds...")

    import aiohttp
    from data.football_fetcher import make_session

    async with make_session() as session:
        odds_data = await get_fixture_odds(
            session,
            pick_data.get("league_id"),
            pick_data.get("home_team", ""),
            pick_data.get("away_team", ""),
            pick_data.get("sport", "football"),
        )

    if not odds_data:
        await _reply(
            update,
            "⚠️ Odds not available for this fixture.",
            back_to_menu(),
        )
        return

    markets = odds_data.get("markets", {})
    lines   = [
        f"📈 <b>Live Odds — "
        f"{odds_data['home_team']} vs "
        f"{odds_data['away_team']}</b>\n"
    ]

    # H2H
    if "h2h" in markets:
        h2h = markets["h2h"]
        lines.append("<b>Match Result:</b>")
        for outcome, price in h2h.items():
            lines.append(f"  {outcome}: <b>{price:.2f}</b>")

    # Totals
    if "totals" in markets:
        lines.append("\n<b>Totals:</b>")
        for outcome, price in markets["totals"].items():
            lines.append(f"  {outcome}: <b>{price:.2f}</b>")

    # Spreads
    if "spreads" in markets:
        lines.append("\n<b>Spreads:</b>")
        for outcome, price in markets["spreads"].items():
            lines.append(f"  {outcome}: <b>{price:.2f}</b>")

    await _reply(
        update, "\n".join(lines), pick_action_keyboard(pick_id)
    )


# ─────────────────────────────────────────
# /bet [pick_id] [stake]
# ─────────────────────────────────────────

async def bet(update: Update,
              context: CallbackContext) -> None:
    """Log a bet on a qualified pick."""
    if len(context.args) < 2:
        await _reply(
            update,
            "Usage: /bet [pick_id] [stake]\n"
            "Example: /bet 7 25",
            back_to_menu(),
        )
        return

    try:
        pick_id = int(context.args[0])
        stake   = float(context.args[1])
    except ValueError:
        await _reply(
            update,
            "❌ Invalid format. "
            "Usage: /bet [pick_id] [stake]"
        )
        return

    if stake <= 0:
        await _reply(update, "❌ Stake must be greater than 0.")
        return

    pick_data = await get_pick(pick_id)
    if not pick_data:
        await _reply(update, f"❌ Pick {pick_id} not found.")
        return

    if pick_data.get("result") != "pending":
        await _reply(
            update,
            "❌ This pick has already been settled."
        )
        return

    odds_val       = pick_data.get("odds") or 2.0
    potential      = round(stake * odds_val, 2)
    market         = pick_data["market"].replace(
        "_", " "
    ).upper()
    home           = pick_data.get("home_team", "Home")
    away           = pick_data.get("away_team", "Away")
    grade          = pick_data.get("confidence", "?")

    text = (
        f"💰 <b>Confirm Bet</b>\n\n"
        f"{_sport_emoji(pick_data.get('sport', 'football'))} "
        f"<b>{home} vs {away}</b>\n"
        f"📌 {market}\n"
        f"{_grade_emoji(grade)} Grade {grade}\n"
        f"@ <b>{odds_val:.2f}</b>\n\n"
        f"Stake: <b>£{stake:.2f}</b>\n"
        f"Potential return: <b>£{potential:.2f}</b>\n"
        f"Profit if wins: <b>£{potential - stake:.2f}</b>"
    )
    await _reply(
        update, text,
        bet_confirm_keyboard(pick_id, stake)
    )


# ─────────────────────────────────────────
# /bets
# ─────────────────────────────────────────

async def bets(update: Update,
               context: CallbackContext) -> None:
    """Show all open bets for the user."""
    user_id   = update.effective_user.id
    open_bets = await get_open_bets(user_id)

    if not open_bets:
        await _reply(
            update,
            "📭 You have no open bets.\n\n"
            "Use /today to see today's picks and "
            "/bet [pick_id] [stake] to log one.",
            back_to_menu(),
        )
        return

    total_staked = sum(b["stake"] for b in open_bets)
    total_pot    = sum(b["potential_return"] for b in open_bets)

    text = (
        f"📋 <b>Open Bets ({len(open_bets)})</b>\n\n"
        f"Total staked: <b>£{total_staked:.2f}</b>\n"
        f"Total potential: <b>£{total_pot:.2f}</b>\n\n"
        f"Tap a bet for details:"
    )
    await _reply(update, text, open_bets_keyboard(open_bets))


# ─────────────────────────────────────────
# /settle
# ─────────────────────────────────────────

async def settle(update: Update,
                 context: CallbackContext) -> None:
    """Manually trigger result check for open bets."""
    text = (
        "🔄 <b>Settle Open Bets</b>\n\n"
        "This will check all finished fixtures and "
        "settle any open bets with known results.\n\n"
        "The bot also runs this automatically every 2 hours."
    )
    await _reply(update, text, settle_confirm_keyboard())


# ─────────────────────────────────────────
# /journal
# ─────────────────────────────────────────

async def journal(update: Update,
                  context: CallbackContext) -> None:
    """Full bet history — open and settled."""
    user_id = update.effective_user.id
    history = await get_bet_history(user_id)

    if not history:
        await _reply(
            update,
            "📭 No bet history yet.\n\n"
            "Use /today and /bet to start tracking.",
            back_to_menu(),
        )
        return

    lines = ["📖 <b>Bet Journal</b>\n"]
    for b in history[:20]:    # cap at 20 for message length
        status_emoji = {
            "won":  "✅", "lost": "❌",
            "open": "⏳", "void": "↩️",
        }.get(b["status"], "❓")

        result_str = ""
        if b["status"] == "won":
            result_str = f" → +£{b['actual_return'] - b['stake']:.2f}"
        elif b["status"] == "lost":
            result_str = f" → -£{b['stake']:.2f}"

        lines.append(
            f"{status_emoji} "
            f"<b>{b['home_team']} vs {b['away_team']}</b>\n"
            f"   {b['market'].replace('_',' ').upper()} "
            f"| £{b['stake']:.2f} @ {b['odds']:.2f}"
            f"{result_str}\n"
        )

    if len(history) > 20:
        lines.append(
            f"\n<i>Showing 20 of {len(history)} bets.</i>"
        )

    await _reply(update, "\n".join(lines), back_to_menu())


# ─────────────────────────────────────────
# /performance
# ─────────────────────────────────────────

async def performance(update: Update,
                       context: CallbackContext) -> None:
    """ROI, win rate, best/worst market, profit by sport."""
    user_id = update.effective_user.id
    stats   = await get_performance_stats(user_id)
    overall = stats.get("overall", {})

    total   = overall.get("total_bets") or 0
    wins    = overall.get("wins") or 0
    losses  = overall.get("losses") or 0
    staked  = overall.get("total_staked") or 0
    ret     = overall.get("total_returned") or 0
    profit  = ret - staked
    roi     = (profit / staked * 100) if staked > 0 else 0
    win_rate = (wins / total * 100) if total > 0 else 0

    lines = [
        "📊 <b>Performance Summary</b>\n",
        f"Total bets:  <b>{total}</b>",
        f"Win rate:    <b>{win_rate:.1f}%</b> "
        f"({wins}W / {losses}L)",
        f"Total staked: <b>£{staked:.2f}</b>",
        f"Total return: <b>£{ret:.2f}</b>",
        f"Profit/Loss:  <b>£{profit:+.2f}</b>",
        f"ROI:          <b>{roi:+.1f}%</b>\n",
    ]

    # Best market
    by_market = stats.get("by_market", [])
    if by_market:
        best = by_market[0]
        lines.append(
            f"🏆 Best market: <b>"
            f"{best['market'].replace('_',' ').upper()}"
            f"</b> ({best['wins']}/{best['count']} wins)"
        )

    # By sport breakdown
    by_sport = stats.get("by_sport", [])
    if by_sport:
        lines.append("\n<b>By Sport:</b>")
        for s in by_sport:
            s_profit = s.get("profit") or 0
            lines.append(
                f"{_sport_emoji(s['sport'])} "
                f"{s['sport'].upper()}: "
                f"{s['wins']}/{s['count']} wins | "
                f"£{s_profit:+.2f}"
            )

    await _reply(update, "\n".join(lines), back_to_menu())


# ─────────────────────────────────────────
# /backtest [sport] [days]
# ─────────────────────────────────────────

async def backtest(update: Update,
                   context: CallbackContext) -> None:
    """Run SHARP v3 backtest over settled picks."""
    if len(context.args) < 2:
        await _reply(
            update,
            "Usage: /backtest [sport] [days]\n"
            "Example: /backtest football 30\n\n"
            "Or use the buttons:",
            backtest_keyboard(),
        )
        return

    sport = context.args[0].lower()
    if sport not in ("football", "nba"):
        await _reply(
            update,
            "❌ Sport must be 'football' or 'nba'",
            backtest_keyboard(),
        )
        return

    try:
        days = int(context.args[1])
        days = max(7, min(90, days))
    except ValueError:
        await _reply(update, "❌ Days must be a number.")
        return

    picks = await get_settled_picks(sport, days)

    if not picks:
        await _reply(
            update,
            f"📭 No settled {sport} picks in the "
            f"last {days} days.\n\n"
            f"The backtest builds as picks settle over time.",
            back_to_menu(),
        )
        return

    # Build DataFrame for analysis
    df = pd.DataFrame(picks)

    total   = len(df)
    won     = len(df[df["result"] == "won"])
    lost    = len(df[df["result"] == "lost"])
    hit_rate = won / total * 100 if total > 0 else 0

    # Per market breakdown
    market_stats = (
        df.groupby("market")
        .agg(
            count=("result", "count"),
            wins=("result", lambda x: (x == "won").sum()),
        )
        .reset_index()
    )
    market_stats["hit_rate"] = (
        market_stats["wins"] /
        market_stats["count"] * 100
    ).round(1)
    market_stats = market_stats.sort_values(
        "hit_rate", ascending=False
    )

    # Per grade breakdown
    grade_stats = (
        df.groupby("confidence")
        .agg(
            count=("result", "count"),
            wins=("result", lambda x: (x == "won").sum()),
        )
        .reset_index()
    )

    lines = [
        f"📊 <b>Backtest — {sport.upper()} "
        f"({days} days)</b>\n",
        f"Total picks:  <b>{total}</b>",
        f"Won:          <b>{won}</b>",
        f"Lost:         <b>{lost}</b>",
        f"Hit rate:     <b>{hit_rate:.1f}%</b>\n",
        "<b>By Market:</b>",
    ]

    for _, row in market_stats.iterrows():
        bar = "█" * int(row["hit_rate"] / 10)
        lines.append(
            f"  {row['market'].replace('_',' ').upper()}\n"
            f"  {bar} {row['hit_rate']}% "
            f"({int(row['wins'])}/{int(row['count'])})"
        )

    lines.append("\n<b>By Grade:</b>")
    for _, row in grade_stats.iterrows():
        g       = row["confidence"]
        g_rate  = row["wins"] / row["count"] * 100
        lines.append(
            f"  {_grade_emoji(g)} Grade {g}: "
            f"{g_rate:.1f}% "
            f"({int(row['wins'])}/{int(row['count'])})"
        )

    await _reply(update, "\n".join(lines), back_to_menu())


# ─────────────────────────────────────────
# /model
# ─────────────────────────────────────────

async def model(update: Update,
                context: CallbackContext) -> None:
    """Display current SHARP v3 thresholds."""
    from config import (
        ORIG_CS_RATE, ORIG_OPP_GOALS_AWAY,
        ORIG_WIN_BY_2_RATE, ORIG_LEAGUE_AVG_GOALS,
        ORIG_HOME_ML_PROB, ORIG_AWAY_ML_PROB,
        ORIG_REST_DAYS_MIN, ORIG_REFEREE_CARD_LIMIT,
        ORIG_MAX_INJURIES, GRADE_A_MIN_ODDS,
        HVLO_BTTS_HIT_RATE, HVLO_OVER_15_RATE,
        HVLO_MIN_LEG_ODDS, HVLO_MAX_LEG_ODDS,
        HVLO_TARGET_ACCA_MIN, HVLO_TARGET_ACCA_MAX,
        NBA_HOME_ML_PROB, NBA_AWAY_ML_PROB,
        NBA_OPP_WIN_PCT, NBA_LOSING_STREAK_MAX,
    )

    text = (
        "🧠 <b>SHARP Model v3 — Active Thresholds</b>\n\n"
        "<b>⚽ Original Mode</b>\n"
        f"Clean sheet rate:    ≥{ORIG_CS_RATE}\n"
        f"Opp goals away:      <{ORIG_OPP_GOALS_AWAY}\n"
        f"Win by 2+ rate:      ≥{ORIG_WIN_BY_2_RATE}\n"
        f"League avg goals:    <{ORIG_LEAGUE_AVG_GOALS}\n"
        f"Home ML prob:        ≥{ORIG_HOME_ML_PROB}\n"
        f"Away ML prob:        ≥{ORIG_AWAY_ML_PROB}\n"
        f"Rest days min:       ≥{ORIG_REST_DAYS_MIN}\n"
        f"Referee card limit:  <{ORIG_REFEREE_CARD_LIMIT}\n"
        f"Max injuries:        <{ORIG_MAX_INJURIES}\n"
        f"Grade A min odds:    >{GRADE_A_MIN_ODDS}\n\n"
        "<b>⚡ HVLO Mode</b>\n"
        f"BTTS hit rate:       ≥{HVLO_BTTS_HIT_RATE}\n"
        f"Over 1.5 rate:       ≥{HVLO_OVER_15_RATE}\n"
        f"Leg odds range:      "
        f"{HVLO_MIN_LEG_ODDS}–{HVLO_MAX_LEG_ODDS}\n"
        f"Target acca odds:    "
        f"{HVLO_TARGET_ACCA_MIN}–{HVLO_TARGET_ACCA_MAX}\n\n"
        "<b>🏀 NBA Mode</b>\n"
        f"Home ML prob:        ≥{NBA_HOME_ML_PROB}\n"
        f"Away ML prob:        ≥{NBA_AWAY_ML_PROB}\n"
        f"Opp win pct:         <{NBA_OPP_WIN_PCT}\n"
        f"Max losing streak:   ≤{NBA_LOSING_STREAK_MAX}\n"
    )
    await _reply(update, text, back_to_menu())


# ─────────────────────────────────────────
# /leagues
# ─────────────────────────────────────────

async def leagues(update: Update,
                  context: CallbackContext) -> None:
    """Show monitored leagues per mode."""
    orig_list = "\n".join(
        f"  • {name}" for name in ORIGINAL_LEAGUES
    )
    hvlo_list = "\n".join(
        f"  • {name}" for name in HVLO_LEAGUES
    )

    text = (
        "🏆 <b>Monitored Leagues</b>\n\n"
        "<b>⚽ Original Mode</b>\n"
        f"{orig_list}\n\n"
        "<b>⚡ HVLO Mode</b>\n"
        f"{hvlo_list}\n\n"
        "<b>🏀 NBA Mode</b>\n"
        "  • NBA (all games)"
    )
    await _reply(update, text, leagues_keyboard())


# ─────────────────────────────────────────
# /setpreference
# ─────────────────────────────────────────

async def setpreference(update: Update,
                         context: CallbackContext) -> None:
    """Change sport preference via inline buttons."""
    text = (
        "🎯 <b>Sport Preference</b>\n\n"
        "Choose which sports to receive picks for.\n"
        "This affects /today and your morning briefing."
    )
    await _reply(update, text, sport_preference_keyboard())


# ─────────────────────────────────────────
# /setgames [8-30]
# ─────────────────────────────────────────

async def setgames(update: Update,
                   context: CallbackContext) -> None:
    """Set minimum games played threshold."""
    if not context.args:
        await _reply(
            update,
            "📏 <b>Minimum Games Threshold</b>\n\n"
            "Sets how many games a team must have played "
            "before the model considers them.\n\n"
            "Higher = stricter = fewer picks.\n"
            "Default: 8 | Max: 30\n\n"
            "Select below or type /setgames [number]:",
            min_games_keyboard(),
        )
        return

    try:
        n = int(context.args[0])
    except ValueError:
        await _reply(update, "❌ Must be a number between 8–30.")
        return

    n = max(8, min(30, n))
    await update_min_games(update.effective_user.id, n)

    await _reply(
        update,
        f"✅ Minimum games threshold set to <b>{n}</b>.\n\n"
        f"Picks will now require teams to have played "
        f"at least {n} games this season.",
        back_to_menu(),
    )


# ─────────────────────────────────────────
# /briefing
# ─────────────────────────────────────────

async def briefing(update: Update,
                   context: CallbackContext) -> None:
    """Manually trigger the morning briefing."""
    await _reply(
        update,
        "🔄 Generating your briefing...\n\n"
        "Fetching today's fixtures and running "
        "SHARP filters now."
    )

    # Import and run the briefing job directly
    from scheduler.jobs import run_morning_briefing
    try:
        await run_morning_briefing(
            context.application, user_id=update.effective_user.id
        )
    except Exception as e:
        logger.error(f"Manual briefing failed: {e}")
        await _reply(
            update,
            "⚠️ Briefing generation failed. "
            "Check that your API keys are configured "
            "and try again.",
            back_to_menu(),
        )


# ─────────────────────────────────────────
# CALLBACK QUERY HANDLER
# Handles all inline button taps
# ─────────────────────────────────────────

async def button_callback(update: Update,
                           context: CallbackContext) -> None:
    """
    Central dispatcher for all inline keyboard button taps.
    Routes callback_data strings to the correct action.
    """
    query = update.callback_query
    data  = query.data

    # ── Navigation ────────────────────────────────────────────────
    if data == "main_menu":
        await start(update, context)

    elif data == "help":
        await help_command(update, context)

    elif data == "settings":
        text = (
            "⚙️ <b>Settings</b>\n\n"
            "Manage your SHARP Bot preferences."
        )
        await _reply(update, text, settings_keyboard())

    elif data == "settings_preference":
        await setpreference(update, context)

    elif data == "settings_mingames":
        await setgames(update, context)

    # ── Sport preference ──────────────────────────────────────────
    elif data.startswith("pref_"):
        pref = data.replace("pref_", "")
        await update_sport_preference(
            update.effective_user.id, pref
        )
        labels = {
            "football": "⚽ Football Only",
            "nba":      "🏀 NBA Only",
            "both":     "🌐 Both Sports",
        }
        await _reply(
            update,
            f"✅ Preference set to "
            f"<b>{labels.get(pref, pref)}</b>",
            main_menu_keyboard(),
        )

    # ── Min games ─────────────────────────────────────────────────
    elif data.startswith("games_"):
        n = int(data.replace("games_", ""))
        await update_min_games(update.effective_user.id, n)
        await _reply(
            update,
            f"✅ Minimum games set to <b>{n}</b>.",
            settings_keyboard(),
        )

    # ── Today's picks ─────────────────────────────────────────────
    elif data in ("today_football", "today_nba", "today_all"):
        sport_map = {
            "today_football": "football",
            "today_nba":      "nba",
            "today_all":      None,
        }
        sport = sport_map[data]
        picks = await get_todays_picks(sport=sport)

        if not picks:
            await _reply(
                update,
                "📭 No picks available for this sport today.",
                back_to_menu(),
            )
            return

        grade_a = sum(
            1 for p in picks if p["confidence"] == GRADE_A
        )
        text = (
            f"📋 <b>Today's Picks</b>\n\n"
            f"🟢 Grade A: {grade_a} of {len(picks)} total\n\n"
            f"Tap a pick for details:"
        )
        await _reply(update, text, picks_keyboard(picks))

    # ── Single pick detail ────────────────────────────────────────
    elif data.startswith("pick_"):
        pick_id   = int(data.split("_")[1])
        pick_data = await get_pick(pick_id)
        if not pick_data:
            await _reply(update, "❌ Pick not found.")
            return
        text = _format_pick(pick_data)
        await _reply(
            update, text, pick_action_keyboard(pick_id)
        )

    # ── Bet flow ──────────────────────────────────────────────────
    elif data.startswith("bet_") and "confirm" not in data:
        pick_id   = int(data.split("_")[1])
        pick_data = await get_pick(pick_id)
        if not pick_data:
            await _reply(update, "❌ Pick not found.")
            return
        await _reply(
            update,
            f"💰 Enter your stake for pick {pick_id}:\n\n"
            f"Type: /bet {pick_id} [stake]\n"
            f"Example: /bet {pick_id} 25",
            back_to_menu(),
        )

    elif data.startswith("confirm_bet_"):
        parts   = data.split("_")
        pick_id = int(parts[2])
        stake   = float(parts[3])

        pick_data = await get_pick(pick_id)
        if not pick_data:
            await _reply(update, "❌ Pick not found.")
            return

        odds_val = pick_data.get("odds") or 2.0
        bet_row  = {
            "user_id":  update.effective_user.id,
            "pick_id":  pick_id,
            "stake":    stake,
            "odds":     odds_val,
        }
        bet_id = await insert_bet(bet_row)
        await _reply(
            update,
            f"✅ <b>Bet logged!</b>\n\n"
            f"Bet ID: <code>{bet_id}</code>\n"
            f"Stake: £{stake:.2f} @ {odds_val:.2f}\n"
            f"Potential: £{stake * odds_val:.2f}\n\n"
            f"Track it with /bets",
            back_to_menu(),
        )

    elif data.startswith("cancel_bet_"):
        await _reply(
            update,
            "❌ Bet cancelled.",
            back_to_menu(),
        )

    # ── My bets ───────────────────────────────────────────────────
    elif data == "my_bets":
        await bets(update, context)

    # ── Performance ───────────────────────────────────────────────
    elif data == "performance":
        await performance(update, context)

    # ── Briefing ──────────────────────────────────────────────────
    elif data == "briefing":
        await briefing(update, context)

    # ── Settle ────────────────────────────────────────────────────
    elif data == "settle":
        await settle(update, context)

    elif data == "confirm_settle":
        from scheduler.jobs import run_result_checker
        try:
            await run_result_checker(context.application)
            await _reply(
                update,
                "✅ Result check complete. "
                "Check /bets for updates.",
                back_to_menu(),
            )
        except Exception as e:
            logger.error(f"Manual settle failed: {e}")
            await _reply(
                update,
                "⚠️ Settle failed. Try again shortly.",
                back_to_menu(),
            )

    # ── Backtest ──────────────────────────────────────────────────
    elif data.startswith("backtest_"):
        parts = data.split("_")
        if parts[1] in ("football", "nba"):
            context.user_data["backtest_sport"] = parts[1]
            await _reply(
                update,
                f"✅ Sport set to {parts[1].upper()}. "
                f"Now select days:",
                backtest_keyboard(),
            )
        elif parts[1] == "days":
            sport = context.user_data.get(
                "backtest_sport", "football"
            )
            days  = int(parts[2])
            context.args = [sport, str(days)]
            await backtest(update, context)

    # ── Leagues ───────────────────────────────────────────────────
    elif data.startswith("leagues_"):
        await leagues(update, context)

    # ── Odds ─────────────────────────────────────────────────────
    elif data.startswith("odds_"):
        pick_id      = int(data.split("_")[1])
        context.args = [str(pick_id)]
        await odds(update, context)

    # ── Bet detail ────────────────────────────────────────────────
    elif data.startswith("bet_detail_"):
        bet_id   = int(data.split("_")[2])
        user_id  = update.effective_user.id
        history  = await get_bet_history(user_id)
        bet_data = next(
            (b for b in history if b["id"] == bet_id), None
        )
        if not bet_data:
            await _reply(update, "❌ Bet not found.")
            return

        status_emoji = {
            "won": "✅", "lost": "❌",
            "open": "⏳", "void": "↩️",
        }.get(bet_data["status"], "❓")

        await _reply(
            update,
            f"{status_emoji} <b>Bet Detail</b>\n\n"
            f"{bet_data['home_team']} vs "
            f"{bet_data['away_team']}\n"
            f"Market: {bet_data['market']}\n"
            f"Stake: £{bet_data['stake']:.2f}\n"
            f"Odds: {bet_data['odds']:.2f}\n"
            f"Potential: £{bet_data['potential_return']:.2f}\n"
            f"Status: {bet_data['status'].upper()}",
            back_to_menu(),
        )