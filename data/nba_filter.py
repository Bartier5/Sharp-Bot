import logging
from dataclasses import dataclass, field
from typing import Optional
from config import (
    NBA_MIN_GAMES,
    NBA_HOME_ML_PROB,
    NBA_AWAY_ML_PROB,
    NBA_OPP_WIN_PCT,
    NBA_SPREAD_MARGIN_CAP,
    NBA_1H_ML_PROB,
    NBA_MAX_PARLAY_LEGS,
    NBA_LOSING_STREAK_MAX,
    NBA_FORM_HOME_WINS,
    NBA_FORM_AVG_MARGIN,
    NBA_OFFENSE_RANK_CUTOFF,
    NBA_MARKETS,
    GRADE_A,
    GRADE_B,
    GRADE_C,
    GRADE_VOID,
    GRADE_A_MIN_ODDS,
    NEAR_MISS_TOLERANCE,
    MODE_NBA,
)
from data.football_filter import QualifiedMarket, FilterResult

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

def evaluate_nba_fixture(
    fixture:          dict,
    home_stats:       dict,
    away_stats:       dict,
    home_record:      dict,
    away_record:      dict,
    home_form:        list[dict],
    away_form:        list[dict],
    home_injuries:    list[dict],
    away_injuries:    list[dict],
    home_top_players: list[dict],
    away_top_players: list[dict],
    offense_rankings: list[dict],
    odds_data:        Optional[dict] = None,
    min_games:        int = NBA_MIN_GAMES,
) -> FilterResult:
    """
    Main entry point for the NBA filter engine.
    Runs the full 4-stage SHARP v3 NBA pipeline.
    Returns a FilterResult with all qualified markets.
    """
    fid = fixture["fixture_id"]

    logger.info(
        f"Evaluating NBA: {fixture['home_team']} vs "
        f"{fixture['away_team']}"
    )

    # ── STAGE 1 — ELIGIBILITY GATE ────────────────────────────────
    gate = _stage1_gate(
        fixture, home_stats, away_stats,
        home_record, away_record,
        home_form, away_form,
        home_injuries, away_injuries,
        home_top_players, away_top_players,
        min_games,
    )

    if not gate["passed"]:
        logger.info(f"[{fid}] NBA VOID at Stage 1 — {gate['reason']}")
        return FilterResult(
            fixture_id=fid,
            mode=MODE_NBA,
            passed=False,
            void_reason=gate["reason"],
            void_stage=1,
        )

    # ── STAGE 2 — MARKET QUALIFICATION ───────────────────────────
    qualified, near_misses = _stage2_markets(
        fixture, home_stats, away_stats,
        home_record, away_record,
        offense_rankings, odds_data,
    )

    if not qualified:
        logger.info(f"[{fid}] No NBA markets qualified at Stage 2")
        return FilterResult(
            fixture_id=fid,
            mode=MODE_NBA,
            passed=False,
            void_reason="no_nba_markets_qualified",
            void_stage=2,
            near_misses=near_misses,
        )

    # ── STAGE 3 — FORM MODIFIER ───────────────────────────────────
    form_grade = _stage3_form(home_form, away_form)

    # ── STAGE 4 — CONFIDENCE GRADING ─────────────────────────────
    final_markets = _stage4_grade(qualified, form_grade, odds_data)

    # Enforce parlay leg cap across all markets
    final_markets = _enforce_parlay_cap(final_markets)

    logger.info(
        f"[{fid}] NBA Passed — "
        f"{len(final_markets)} markets qualified"
    )

    return FilterResult(
        fixture_id=fid,
        mode=MODE_NBA,
        passed=True,
        qualified_markets=final_markets,
        form_grade=form_grade,
        near_misses=near_misses,
    )


# ─────────────────────────────────────────
# STAGE 1 — ELIGIBILITY GATE
# ─────────────────────────────────────────

def _stage1_gate(
    fixture:          dict,
    home_stats:       dict,
    away_stats:       dict,
    home_record:      dict,
    away_record:      dict,
    home_form:        list[dict],
    away_form:        list[dict],
    home_injuries:    list[dict],
    away_injuries:    list[dict],
    home_top_players: list[dict],
    away_top_players: list[dict],
    min_games:        int,
) -> dict:
    """
    Hard eligibility checks for NBA fixtures.
    Returns {"passed": bool, "reason": str}.
    First failure returns immediately.
    """

    # ── 1. Minimum games played ───────────────────────────────────
    home_played = home_stats.get("games_played_total", 0)
    away_played = away_stats.get("games_played_total", 0)

    if home_played < min_games:
        return _void(
            f"home_insufficient_games:{home_played}<{min_games}"
        )
    if away_played < min_games:
        return _void(
            f"away_insufficient_games:{away_played}<{min_games}"
        )

    # ── 2. Opponent win percentage ────────────────────────────────
    # At least one team's opponent must be weak enough
    away_win_pct = away_record.get("win_pct", 1.0)
    home_win_pct = home_record.get("win_pct", 1.0)

    # The AWAY team is the opponent when home team is favoured
    # The HOME team is the opponent when away team is favoured
    # Both must be checked — we qualify picks for both sides
    if away_win_pct >= NBA_OPP_WIN_PCT and \
       home_win_pct >= NBA_OPP_WIN_PCT:
        return _void(
            f"both_teams_above_win_pct_threshold:"
            f"home_{home_win_pct}_away_{away_win_pct}"
        )

    # ── 3. Losing streak check ────────────────────────────────────
    from data.nba_fetcher import calculate_losing_streak

    home_streak = calculate_losing_streak(home_form)
    away_streak = calculate_losing_streak(away_form)

    if home_streak > NBA_LOSING_STREAK_MAX:
        return _void(
            f"home_losing_streak:{home_streak}"
        )
    if away_streak > NBA_LOSING_STREAK_MAX:
        return _void(
            f"away_losing_streak:{away_streak}"
        )

    # ── 4. Star player availability ───────────────────────────────
    # Check home team's top 2 players by minutes
    for star in home_top_players:
        player_id = star.get("player_id")
        for injury in home_injuries:
            if injury.get("player_id") == player_id:
                return _void(
                    f"home_star_out:{star['name']}"
                    f"_status:{injury['status']}"
                )

    # Check away team's top 2 players by minutes
    for star in away_top_players:
        player_id = star.get("player_id")
        for injury in away_injuries:
            if injury.get("player_id") == player_id:
                return _void(
                    f"away_star_out:{star['name']}"
                    f"_status:{injury['status']}"
                )

    return {"passed": True, "reason": None}


# ─────────────────────────────────────────
# STAGE 2 — MARKET QUALIFICATION
# ─────────────────────────────────────────

def _stage2_markets(
    fixture:          dict,
    home_stats:       dict,
    away_stats:       dict,
    home_record:      dict,
    away_record:      dict,
    offense_rankings: list[dict],
    odds_data:        Optional[dict],
) -> tuple[list[QualifiedMarket], list[dict]]:
    """
    Evaluate each NBA SHARP market independently.
    Returns (qualified_markets, near_misses).
    """
    qualified   = []
    near_misses = []

    def is_near_miss(value: float, threshold: float) -> bool:
        if threshold == 0:
            return False
        return (
            abs(value - threshold) / threshold
            <= NEAR_MISS_TOLERANCE
        )

    # Pull implied probabilities from odds if available
    home_impl_prob = 0.0
    away_impl_prob = 0.0

    if odds_data:
        from data.odds_fetcher import (
            get_home_win_odds,
            get_away_win_odds,
            odds_to_implied_prob,
        )
        home_odds = get_home_win_odds(odds_data)
        away_odds = get_away_win_odds(odds_data)
        if home_odds:
            home_impl_prob = odds_to_implied_prob(home_odds)
        if away_odds:
            away_impl_prob = odds_to_implied_prob(away_odds)

    away_win_pct = away_record.get("win_pct", 1.0)
    home_win_pct = home_record.get("win_pct", 1.0)

    # ── MARKET 1: Moneyline — Home ────────────────────────────────
    ml_home_flags = {
        "implied_prob":    home_impl_prob,
        "threshold":       NBA_HOME_ML_PROB,
        "opp_win_pct":     away_win_pct,
        "opp_pct_pass":    away_win_pct < NBA_OPP_WIN_PCT,
        "prob_pass":       home_impl_prob >= NBA_HOME_ML_PROB,
    }

    if ml_home_flags["prob_pass"] and ml_home_flags["opp_pct_pass"]:
        qualified.append(QualifiedMarket(
            market="nba_ml_home",
            confidence=GRADE_A,
            qualifier_flags=ml_home_flags,
            odds=_get_home_odds(odds_data),
        ))
    elif is_near_miss(home_impl_prob, NBA_HOME_ML_PROB):
        near_misses.append({
            "market": "nba_ml_home",
            "flags":  ml_home_flags,
        })

    # ── MARKET 2: Moneyline — Away ────────────────────────────────
    ml_away_flags = {
        "implied_prob":    away_impl_prob,
        "threshold":       NBA_AWAY_ML_PROB,
        "opp_win_pct":     home_win_pct,
        "opp_pct_pass":    home_win_pct < NBA_OPP_WIN_PCT,
        "prob_pass":       away_impl_prob >= NBA_AWAY_ML_PROB,
    }

    if ml_away_flags["prob_pass"] and ml_away_flags["opp_pct_pass"]:
        qualified.append(QualifiedMarket(
            market="nba_ml_away",
            confidence=GRADE_A,
            qualifier_flags=ml_away_flags,
            odds=_get_away_odds(odds_data),
        ))
    elif is_near_miss(away_impl_prob, NBA_AWAY_ML_PROB):
        near_misses.append({
            "market": "nba_ml_away",
            "flags":  ml_away_flags,
        })

    # ── MARKET 3: Spread ──────────────────────────────────────────
    home_margin = home_stats.get("avg_winning_margin", 0)
    spread_cap  = home_margin * NBA_SPREAD_MARGIN_CAP

    # Get spread line from odds if available
    spread_line = _get_spread_line(odds_data, fixture["home_team"])

    spread_flags = {
        "avg_winning_margin": home_margin,
        "spread_cap":         round(spread_cap, 2),
        "spread_line":        spread_line,
        "opp_win_pct":        away_win_pct,
        "opp_pct_pass":       away_win_pct < NBA_OPP_WIN_PCT,
        "spread_pass": (
            spread_line is not None and
            abs(spread_line) <= spread_cap
        ),
    }

    if (spread_flags["spread_pass"] and
            spread_flags["opp_pct_pass"] and
            home_margin > 0):
        qualified.append(QualifiedMarket(
            market="nba_spread",
            confidence=GRADE_A,
            qualifier_flags=spread_flags,
        ))
    else:
        if spread_line and is_near_miss(
            abs(spread_line), spread_cap
        ):
            near_misses.append({
                "market": "nba_spread",
                "flags":  spread_flags,
            })

    # ── MARKET 4: 1st Half ML ─────────────────────────────────────
    home_ht_win_rate = home_stats.get("home_ht_win_rate", 0)

    h1_flags = {
        "implied_prob":      home_impl_prob,
        "ht_win_rate":       home_ht_win_rate,
        "prob_threshold":    NBA_1H_ML_PROB,
        "ht_rate_threshold": 0.60,
        "prob_pass":         home_impl_prob >= NBA_1H_ML_PROB,
        "ht_rate_pass":      home_ht_win_rate >= 0.60,
        "opp_pct_pass":      away_win_pct < NBA_OPP_WIN_PCT,
    }

    if (h1_flags["prob_pass"] and
            h1_flags["ht_rate_pass"] and
            h1_flags["opp_pct_pass"]):
        qualified.append(QualifiedMarket(
            market="nba_1h_ml",
            confidence=GRADE_A,
            qualifier_flags=h1_flags,
        ))
    elif is_near_miss(home_impl_prob, NBA_1H_ML_PROB):
        near_misses.append({
            "market": "nba_1h_ml",
            "flags":  h1_flags,
        })

    # ── MARKET 5: Team Total OVER ─────────────────────────────────
    from data.nba_fetcher import get_offense_rank

    home_rank       = get_offense_rank(offense_rankings,
                                        fixture.get("home_id", 0))
    home_avg_scored = home_stats.get("avg_pts_scored_home", 0)
    home_score_rate = home_stats.get(
        "home_above_avg_rate", 0
    )  # % games scored >= season avg

    tt_flags = {
        "offense_rank":       home_rank,
        "rank_threshold":     NBA_OFFENSE_RANK_CUTOFF,
        "rank_pass":          home_rank <= NBA_OFFENSE_RANK_CUTOFF,
        "avg_pts_home":       home_avg_scored,
        "score_rate":         home_score_rate,
        "score_rate_pass":    home_score_rate >= 0.60,
        "opp_defense_below_median": away_stats.get(
            "defense_below_median", False
        ),
    }

    if (tt_flags["rank_pass"] and
            tt_flags["score_rate_pass"] and
            tt_flags["opp_defense_below_median"]):
        qualified.append(QualifiedMarket(
            market="nba_team_total_over",
            confidence=GRADE_A,
            qualifier_flags=tt_flags,
        ))
    elif is_near_miss(home_rank, NBA_OFFENSE_RANK_CUTOFF):
        near_misses.append({
            "market": "nba_team_total_over",
            "flags":  tt_flags,
        })

    # ── MARKET 6: Parlay Leg Flag ─────────────────────────────────
    # A pick is flagged as a parlay leg if:
    # home team, implied prob >= 80%, all conditions clear
    parlay_flags = {
        "implied_prob":   home_impl_prob,
        "threshold":      NBA_HOME_ML_PROB,
        "is_home":        True,
        "prob_pass":      home_impl_prob >= NBA_HOME_ML_PROB,
        "opp_pct_pass":   away_win_pct < NBA_OPP_WIN_PCT,
        "streak_ok":      True,   # already checked in Stage 1
    }

    if (parlay_flags["prob_pass"] and
            parlay_flags["opp_pct_pass"]):
        qualified.append(QualifiedMarket(
            market="nba_parlay_leg",
            confidence=GRADE_A,
            qualifier_flags=parlay_flags,
            odds=_get_home_odds(odds_data),
        ))

    return qualified, near_misses


# ─────────────────────────────────────────
# STAGE 3 — FORM MODIFIER
# ─────────────────────────────────────────

def _stage3_form(
    home_form: list[dict],
    away_form: list[dict],
) -> str:
    """
    NBA form checks — last 5 games.
    Returns GRADE_A or GRADE_B.
    Does not void — only downgrades confidence.
    """
    if not home_form:
        return GRADE_B

    from data.nba_fetcher import calculate_avg_margin

    # Check 1 — home team won 3+ of last 5
    home_wins = sum(1 for g in home_form if g.get("won"))
    if home_wins < NBA_FORM_HOME_WINS:
        logger.info(
            f"NBA form downgrade — home wins "
            f"{home_wins} < {NBA_FORM_HOME_WINS}"
        )
        return GRADE_B

    # Check 2 — avg winning margin >= 5 points
    avg_margin = calculate_avg_margin(home_form)
    if avg_margin < NBA_FORM_AVG_MARGIN:
        logger.info(
            f"NBA form downgrade — avg margin "
            f"{avg_margin} < {NBA_FORM_AVG_MARGIN}"
        )
        return GRADE_B

    return GRADE_A


# ─────────────────────────────────────────
# STAGE 4 — CONFIDENCE GRADING
# ─────────────────────────────────────────

def _stage4_grade(
    qualified:  list[QualifiedMarket],
    form_grade: str,
    odds_data:  Optional[dict],
) -> list[QualifiedMarket]:
    """
    Apply final confidence grades to all qualified NBA markets.
    Same grading logic as football — form + odds + line movement.
    """
    from data.odds_fetcher import (
        detect_line_movement,
        assess_odds_grade,
        odds_to_implied_prob,
    )

    graded = []

    for market in qualified:
        grade   = GRADE_A
        reasons = []

        # Form downgrade
        if form_grade == GRADE_B:
            grade = GRADE_B
            reasons.append("form_downgrade")

        # Odds quality check
        if market.odds:
            implied_prob = odds_to_implied_prob(market.odds)
            odds_grade   = assess_odds_grade(
                market.odds, implied_prob
            )

            if odds_grade == GRADE_C:
                grade = GRADE_C
                reasons.append("edge_priced_in")
            elif odds_grade == GRADE_B and grade == GRADE_A:
                grade = GRADE_B
                reasons.append("odds_below_threshold")

            # Line movement
            if market.opening_odds and market.odds:
                movement = detect_line_movement(
                    market.opening_odds,
                    market.odds,
                )
                if movement["against_pick"]:
                    if grade == GRADE_A:
                        grade = GRADE_B
                    reasons.append(
                        f"line_moved_"
                        f"{movement['movement_pct']}pct"
                    )

        market.confidence    = grade
        market.grade_reasons = reasons
        graded.append(market)

    return graded


# ─────────────────────────────────────────
# PARLAY CAP ENFORCEMENT
# ─────────────────────────────────────────

def _enforce_parlay_cap(
        markets: list[QualifiedMarket]) -> list[QualifiedMarket]:
    """
    Enforce NBA_MAX_PARLAY_LEGS = 3 across all parlay leg picks.
    Keeps only the 3 highest-confidence parlay legs.
    Non-parlay markets are unaffected.
    """
    parlay_legs  = [
        m for m in markets
        if m.market == "nba_parlay_leg"
    ]
    other_markets = [
        m for m in markets
        if m.market != "nba_parlay_leg"
    ]

    # Grade priority: A > B > C
    grade_order = {GRADE_A: 0, GRADE_B: 1, GRADE_C: 2}
    parlay_legs.sort(
        key=lambda m: grade_order.get(m.confidence, 3)
    )

    # Keep only max allowed legs
    parlay_legs = parlay_legs[:NBA_MAX_PARLAY_LEGS]

    return other_markets + parlay_legs


# ─────────────────────────────────────────
# RESULT SETTLEMENT
# ─────────────────────────────────────────

def settle_nba_market(
    market:     str,
    home_score: int,
    away_score: int,
    ht_home:    Optional[int] = None,
    ht_away:    Optional[int] = None,
    spread:     Optional[float] = None,
) -> str:
    """
    Determine win/loss/void for an NBA market given final scores.
    Returns 'won', 'lost', or 'void'.
    """
    if market == "nba_ml_home":
        return "won" if home_score > away_score else "lost"

    elif market == "nba_ml_away":
        return "won" if away_score > home_score else "lost"

    elif market == "nba_spread":
        if spread is None:
            return "void"
        # Negative spread means home team favoured
        # Home covers if margin > abs(spread)
        margin = home_score - away_score
        return "won" if margin > abs(spread) else "lost"

    elif market == "nba_1h_ml":
        if ht_home is None or ht_away is None:
            return "void"
        return "won" if ht_home > ht_away else "lost"

    elif market == "nba_team_total_over":
        # Team total line approximated from full game
        # Ideally use actual team total line from odds
        avg = home_score
        return "won" if avg > 110 else "lost"

    elif market == "nba_parlay_leg":
        return "won" if home_score > away_score else "lost"

    return "void"


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def _void(reason: str) -> dict:
    """Standardized void dict for Stage 1 failures."""
    return {"passed": False, "reason": reason}


def _get_home_odds(odds_data: Optional[dict]) -> Optional[float]:
    """Safely extract home win odds from odds_data."""
    if not odds_data:
        return None
    try:
        from data.odds_fetcher import get_home_win_odds
        return get_home_win_odds(odds_data)
    except Exception:
        return None


def _get_away_odds(odds_data: Optional[dict]) -> Optional[float]:
    """Safely extract away win odds from odds_data."""
    if not odds_data:
        return None
    try:
        from data.odds_fetcher import get_away_win_odds
        return get_away_win_odds(odds_data)
    except Exception:
        return None


def _get_spread_line(odds_data: Optional[dict],
                     team_name: str) -> Optional[float]:
    """
    Extract spread line value for a team from odds_data.
    Returns the numeric spread line or None.
    """
    if not odds_data:
        return None
    try:
        spreads = odds_data.get("markets", {}).get("spreads", {})
        entry   = spreads.get(team_name)
        if isinstance(entry, (list, tuple)):
            return float(entry[0])   # (line, odds) tuple
        return None
    except Exception:
        return None