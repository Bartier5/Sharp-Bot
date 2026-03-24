import logging
from dataclasses import dataclass, field
from typing import Optional
from config import (
    # Original mode thresholds
    ORIG_MIN_GAMES,
    ORIG_REST_DAYS_MIN,
    ORIG_REFEREE_CARD_LIMIT,
    ORIG_CS_RATE,
    ORIG_OPP_GOALS_AWAY,
    ORIG_WIN_BY_2_RATE,
    ORIG_LEAGUE_AVG_GOALS,
    ORIG_HOME_2H_SCORE_RATE,
    ORIG_AWAY_2H_CONCEDE,
    ORIG_HT_LEAD_RATE,
    ORIG_HT_WIN_FROM_LEAD,
    ORIG_BTTS_HOME_SCORE,
    ORIG_BTTS_AWAY_SCORE,
    ORIG_NO_TEAM_3_LIMIT,
    ORIG_HOME_AVG_GOALS,
    ORIG_HOME_ML_PROB,
    ORIG_AWAY_ML_PROB,
    ORIG_FORM_HOME_WINS,
    ORIG_FORM_AWAY_LOSSES,
    ORIG_MAX_INJURIES,
    ORIG_KEY_POSITIONS,
    ORIG_LINE_MOVE_THRESHOLD,
    ORIG_EDGE_CEILING,
    # HVLO mode thresholds
    HVLO_MIN_GAMES,
    HVLO_BTTS_HIT_RATE,
    HVLO_OVER_15_RATE,
    HVLO_OVER_25_RATE,
    HVLO_MIN_LEG_ODDS,
    HVLO_MAX_LEG_ODDS,
    HVLO_ACCA_MIN_LEGS,
    HVLO_ACCA_MAX_LEGS,
    HVLO_TARGET_ACCA_MIN,
    HVLO_TARGET_ACCA_MAX,
    HVLO_MARKETS,
    # Grading
    GRADE_A,
    GRADE_B,
    GRADE_C,
    GRADE_VOID,
    GRADE_A_MIN_ODDS,
    NEAR_MISS_TOLERANCE,
    # Modes
    MODE_ORIGINAL,
    MODE_HVLO,
    DERBY_FIXTURE_IDS,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────

@dataclass
class QualifiedMarket:
    """
    Represents a single market that passed SHARP filters.
    One fixture can produce multiple QualifiedMarket instances.
    """
    market:           str
    confidence:       str
    qualifier_flags:  dict
    odds:             Optional[float] = None
    opening_odds:     Optional[float] = None
    grade_reasons:    list = field(default_factory=list)


@dataclass
class FilterResult:
    """
    Full result of running a fixture through the filter engine.
    Contains the stage that killed it (if any) and all qualified markets.
    """
    fixture_id:       str
    mode:             str
    passed:           bool
    void_reason:      Optional[str]    = None
    void_stage:       Optional[int]    = None
    qualified_markets: list            = field(default_factory=list)
    form_grade:       str              = GRADE_A
    near_misses:      list             = field(default_factory=list)


# ─────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────

def evaluate_fixture(
    fixture:       dict,
    home_stats:    dict,
    away_stats:    dict,
    standings:     list[dict],
    home_form:     list[dict],
    away_form:     list[dict],
    injuries:      list[dict],
    league_avg:    float,
    win_by_2_rate: float,
    referee_cards: Optional[float],
    odds_data:     Optional[dict],
    min_games:     int = ORIG_MIN_GAMES,
) -> FilterResult:
    """
    Main entry point for the football filter engine.
    Routes to Original or HVLO pipeline based on fixture mode.
    Returns a FilterResult with all qualified markets.
    """
    mode       = fixture.get("mode", MODE_ORIGINAL)
    fixture_id = fixture["fixture_id"]

    logger.info(
        f"Evaluating {fixture['home_team']} vs "
        f"{fixture['away_team']} — mode: {mode}"
    )

    if mode == MODE_HVLO:
        return _evaluate_hvlo(
            fixture, home_stats, away_stats,
            home_form, away_form, odds_data, min_games
        )

    # Default — Original mode
    return _evaluate_original(
        fixture, home_stats, away_stats, standings,
        home_form, away_form, injuries, league_avg,
        win_by_2_rate, referee_cards, odds_data, min_games
    )


# ─────────────────────────────────────────
# ORIGINAL MODE — FULL 4-STAGE PIPELINE
# ─────────────────────────────────────────

def _evaluate_original(
    fixture:       dict,
    home_stats:    dict,
    away_stats:    dict,
    standings:     list[dict],
    home_form:     list[dict],
    away_form:     list[dict],
    injuries:      list[dict],
    league_avg:    float,
    win_by_2_rate: float,
    referee_cards: Optional[float],
    odds_data:     Optional[dict],
    min_games:     int,
) -> FilterResult:
    """
    Runs a fixture through the full Original mode 4-stage pipeline.
    Returns immediately with VOID if any Stage 1 gate fails.
    """
    fid = fixture["fixture_id"]

    # ── STAGE 1 — ELIGIBILITY GATE ────────────────────────────────
    gate = _stage1_gate(
        fixture, home_stats, away_stats,
        standings, injuries, referee_cards, min_games
    )
    if not gate["passed"]:
        logger.info(
            f"[{fid}] VOID at Stage 1 — {gate['reason']}"
        )
        return FilterResult(
            fixture_id=fid,
            mode=MODE_ORIGINAL,
            passed=False,
            void_reason=gate["reason"],
            void_stage=1,
        )

    # ── STAGE 2 — MARKET QUALIFICATION ───────────────────────────
    qualified, near_misses = _stage2_markets(
        fixture, home_stats, away_stats,
        league_avg, win_by_2_rate, odds_data
    )

    if not qualified:
        logger.info(f"[{fid}] No markets qualified at Stage 2")
        return FilterResult(
            fixture_id=fid,
            mode=MODE_ORIGINAL,
            passed=False,
            void_reason="no_markets_qualified",
            void_stage=2,
            near_misses=near_misses,
        )

    # ── STAGE 3 — FORM MODIFIER ───────────────────────────────────
    form_grade = _stage3_form(home_form, away_form)

    # ── STAGE 4 — CONFIDENCE GRADING ─────────────────────────────
    final_markets = _stage4_grade(qualified, form_grade, odds_data)

    logger.info(
        f"[{fid}] Passed — {len(final_markets)} markets qualified"
    )

    return FilterResult(
        fixture_id=fid,
        mode=MODE_ORIGINAL,
        passed=True,
        qualified_markets=final_markets,
        form_grade=form_grade,
        near_misses=near_misses,
    )


# ─────────────────────────────────────────
# STAGE 1 — ELIGIBILITY GATE
# ─────────────────────────────────────────

def _stage1_gate(
    fixture:       dict,
    home_stats:    dict,
    away_stats:    dict,
    standings:     list[dict],
    injuries:      list[dict],
    referee_cards: Optional[float],
    min_games:     int,
) -> dict:
    """
    Hard eligibility checks. First failure immediately returns.
    Returns {"passed": bool, "reason": str}.
    """

    # ── 1. League whitelist ───────────────────────────────────────
    # Already guaranteed by fetcher — only monitored leagues fetched

    # ── 2. Minimum games played ───────────────────────────────────
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

    # ── 3. Standings position ─────────────────────────────────────
    league_size  = len(standings)
    home_team_id = fixture.get("home_id")
    away_team_id = fixture.get("away_id")

    home_pos = _get_position(standings, home_team_id)
    away_pos = _get_position(standings, away_team_id)

    if home_pos is None or away_pos is None:
        return _void("standings_data_missing")

    if home_pos > 2:
        return _void(
            f"home_not_top2:position_{home_pos}"
        )

    bottom_half_threshold = league_size // 2
    if away_pos <= bottom_half_threshold:
        return _void(
            f"away_not_bottom_half:position_{away_pos}"
            f"_of_{league_size}"
        )

    # ── 4. Derby / rivalry check ──────────────────────────────────
    fixture_id = fixture.get("fixture_id")
    if fixture_id in DERBY_FIXTURE_IDS:
        return _void("derby_fixture")

    # ── 5. Rest days ──────────────────────────────────────────────
    home_rest = fixture.get("home_rest_days", 99)
    away_rest = fixture.get("away_rest_days", 99)

    if home_rest < ORIG_REST_DAYS_MIN:
        return _void(
            f"home_fatigue:rest_{home_rest}days"
        )
    if away_rest < ORIG_REST_DAYS_MIN:
        return _void(
            f"away_fatigue:rest_{away_rest}days"
        )

    # ── 6. Referee chaos ──────────────────────────────────────────
    if referee_cards is not None:
        if referee_cards >= ORIG_REFEREE_CARD_LIMIT:
            return _void(
                f"chaos_referee:{referee_cards}cards/game"
            )

    # ── 7. Injury gate ────────────────────────────────────────────
    home_injuries = [
        i for i in injuries
        if i.get("team_id") == home_team_id
    ]

    if len(home_injuries) >= ORIG_MAX_INJURIES:
        return _void(
            f"home_injuries:{len(home_injuries)}_missing"
        )

    for player in home_injuries:
        position = player.get("position", "")
        if position in ORIG_KEY_POSITIONS:
            return _void(
                f"key_player_missing:{position}"
            )

    return {"passed": True, "reason": None}


# ─────────────────────────────────────────
# STAGE 2 — MARKET QUALIFICATION
# ─────────────────────────────────────────

def _stage2_markets(
    fixture:       dict,
    home_stats:    dict,
    away_stats:    dict,
    league_avg:    float,
    win_by_2_rate: float,
    odds_data:     Optional[dict],
) -> tuple[list[QualifiedMarket], list[dict]]:
    """
    Evaluate each SHARP market independently.
    Returns (qualified_markets, near_misses).
    A near-miss is a market where one value was within
    NEAR_MISS_TOLERANCE of the threshold.
    """
    qualified   = []
    near_misses = []

    # Helper — check if value is within tolerance of threshold
    def is_near_miss(value: float, threshold: float) -> bool:
        if threshold == 0:
            return False
        gap = abs(value - threshold) / threshold
        return gap <= NEAR_MISS_TOLERANCE

    # ── MARKET 1: Under 1.5 Goals 0-20min ────────────────────────
    home_conceded_home = home_stats.get(
        "avg_goals_conceded_home", 0
    )
    away_scored_away   = away_stats.get(
        "avg_goals_scored_away", 0
    )

    u15_flags = {
        "league_avg_goals":    league_avg,
        "league_avg_pass":     league_avg < ORIG_LEAGUE_AVG_GOALS,
        "home_conceded_home":  home_conceded_home,
        "home_concede_pass":   home_conceded_home < 1.0,
        "away_scored_away":    away_scored_away,
        "away_score_pass":     away_scored_away < 0.8,
    }

    if (u15_flags["league_avg_pass"] and
            u15_flags["home_concede_pass"] and
            u15_flags["away_score_pass"]):
        qualified.append(QualifiedMarket(
            market="under_1.5_0_20min",
            confidence=GRADE_A,
            qualifier_flags=u15_flags,
        ))
    else:
        # Check near-miss
        if (is_near_miss(league_avg, ORIG_LEAGUE_AVG_GOALS) or
                is_near_miss(home_conceded_home, 1.0) or
                is_near_miss(away_scored_away, 0.8)):
            near_misses.append({
                "market": "under_1.5_0_20min",
                "flags":  u15_flags,
            })

    # ── MARKET 2: Win to Nil ──────────────────────────────────────
    cs_rate_home       = home_stats.get("cs_rate_home", 0)
    away_goals_avg     = away_stats.get("avg_goals_scored_away", 0)
    home_wtn_rate      = home_stats.get("win_to_nil_rate", 0)

    wtn_flags = {
        "cs_rate_home":     cs_rate_home,
        "cs_rate_pass":     cs_rate_home >= ORIG_CS_RATE,
        "away_goals_avg":   away_goals_avg,
        "away_goals_pass":  away_goals_avg < ORIG_OPP_GOALS_AWAY,
        "wtn_rate":         home_wtn_rate,
        "wtn_rate_pass":    home_wtn_rate >= 0.40,
    }

    if (wtn_flags["cs_rate_pass"] and
            wtn_flags["away_goals_pass"] and
            wtn_flags["wtn_rate_pass"]):
        qualified.append(QualifiedMarket(
            market="win_to_nil",
            confidence=GRADE_A,
            qualifier_flags=wtn_flags,
        ))
    else:
        if (is_near_miss(cs_rate_home, ORIG_CS_RATE) or
                is_near_miss(away_goals_avg, ORIG_OPP_GOALS_AWAY)):
            near_misses.append({
                "market": "win_to_nil",
                "flags":  wtn_flags,
            })

    # ── MARKET 3: 2nd Half Over 0.5 (anchor) ─────────────────────
    home_2h_rate   = home_stats.get("home_2h_scoring_rate", 0)
    away_2h_concede = away_stats.get("away_2h_concede_rate", 0)

    h2_flags = {
        "home_2h_scoring_rate":  home_2h_rate,
        "home_2h_pass":          home_2h_rate >= ORIG_HOME_2H_SCORE_RATE,
        "away_2h_concede_rate":  away_2h_concede,
        "away_2h_pass":          away_2h_concede >= ORIG_AWAY_2H_CONCEDE,
    }

    if h2_flags["home_2h_pass"] and h2_flags["away_2h_pass"]:
        qualified.append(QualifiedMarket(
            market="2h_over_0.5",
            confidence=GRADE_A,
            qualifier_flags=h2_flags,
        ))
    else:
        if (is_near_miss(home_2h_rate, ORIG_HOME_2H_SCORE_RATE) or
                is_near_miss(away_2h_concede, ORIG_AWAY_2H_CONCEDE)):
            near_misses.append({
                "market": "2h_over_0.5",
                "flags":  h2_flags,
            })

    # ── MARKET 4: Asian Handicap -1.5 ────────────────────────────
    home_avg_goals = home_stats.get("avg_goals_scored_home", 0)

    ah_flags = {
        "win_by_2_rate":       win_by_2_rate,
        "win_by_2_pass":       win_by_2_rate >= ORIG_WIN_BY_2_RATE,
        "home_avg_goals":      home_avg_goals,
        "home_avg_goals_pass": home_avg_goals >= ORIG_HOME_AVG_GOALS,
    }

    if ah_flags["win_by_2_pass"] and ah_flags["home_avg_goals_pass"]:
        qualified.append(QualifiedMarket(
            market="ah_minus_1.5",
            confidence=GRADE_A,
            qualifier_flags=ah_flags,
        ))
    else:
        if is_near_miss(win_by_2_rate, ORIG_WIN_BY_2_RATE):
            near_misses.append({
                "market": "ah_minus_1.5",
                "flags":  ah_flags,
            })

    # ── MARKET 5: HT/FT (Home/Home) ──────────────────────────────
    ht_lead_rate     = home_stats.get("ht_lead_rate", 0)
    ht_win_from_lead = home_stats.get("ht_win_from_lead_rate", 0)

    htft_flags = {
        "ht_lead_rate":       ht_lead_rate,
        "ht_lead_pass":       ht_lead_rate >= ORIG_HT_LEAD_RATE,
        "ht_win_from_lead":   ht_win_from_lead,
        "ht_win_pass":        ht_win_from_lead >= ORIG_HT_WIN_FROM_LEAD,
    }

    if htft_flags["ht_lead_pass"] and htft_flags["ht_win_pass"]:
        qualified.append(QualifiedMarket(
            market="ht_ft",
            confidence=GRADE_A,
            qualifier_flags=htft_flags,
        ))
    else:
        if (is_near_miss(ht_lead_rate, ORIG_HT_LEAD_RATE) or
                is_near_miss(ht_win_from_lead, ORIG_HT_WIN_FROM_LEAD)):
            near_misses.append({
                "market": "ht_ft",
                "flags":  htft_flags,
            })

    # ── MARKET 6: No Team Scores 3+ ──────────────────────────────
    home_avg_all = home_stats.get("avg_goals_scored_home", 0)
    away_avg_all = away_stats.get("avg_goals_scored_away", 0)

    nt3_flags = {
        "home_avg_goals": home_avg_all,
        "home_pass":      home_avg_all < ORIG_NO_TEAM_3_LIMIT,
        "away_avg_goals": away_avg_all,
        "away_pass":      away_avg_all < ORIG_NO_TEAM_3_LIMIT,
    }

    if nt3_flags["home_pass"] and nt3_flags["away_pass"]:
        qualified.append(QualifiedMarket(
            market="no_team_3plus",
            confidence=GRADE_A,
            qualifier_flags=nt3_flags,
        ))
    else:
        if (is_near_miss(home_avg_all, ORIG_NO_TEAM_3_LIMIT) or
                is_near_miss(away_avg_all, ORIG_NO_TEAM_3_LIMIT)):
            near_misses.append({
                "market": "no_team_3plus",
                "flags":  nt3_flags,
            })

    # ── MARKET 7: BTTS ────────────────────────────────────────────
    home_score_rate = home_stats.get("home_scoring_rate", 0)
    away_score_rate = away_stats.get("away_scoring_rate", 0)

    btts_flags = {
        "home_score_rate": home_score_rate,
        "home_pass":       home_score_rate >= ORIG_BTTS_HOME_SCORE,
        "away_score_rate": away_score_rate,
        "away_pass":       away_score_rate >= ORIG_BTTS_AWAY_SCORE,
    }

    if btts_flags["home_pass"] and btts_flags["away_pass"]:
        qualified.append(QualifiedMarket(
            market="btts",
            confidence=GRADE_A,
            qualifier_flags=btts_flags,
        ))
    else:
        if (is_near_miss(home_score_rate, ORIG_BTTS_HOME_SCORE) or
                is_near_miss(away_score_rate, ORIG_BTTS_AWAY_SCORE)):
            near_misses.append({
                "market": "btts",
                "flags":  btts_flags,
            })

    # ── MARKET 8: Match Line (ML) ─────────────────────────────────
    if odds_data:
        from data.odds_fetcher import (
            get_home_win_odds,
            get_away_win_odds,
            odds_to_implied_prob,
        )

        home_odds = get_home_win_odds(odds_data)
        away_odds = get_away_win_odds(odds_data)

        if home_odds:
            home_prob = odds_to_implied_prob(home_odds)
            ml_home_flags = {
                "implied_prob":  home_prob,
                "threshold":     ORIG_HOME_ML_PROB,
                "odds":          home_odds,
                "pass":          home_prob >= ORIG_HOME_ML_PROB,
            }
            if ml_home_flags["pass"]:
                qualified.append(QualifiedMarket(
                    market="ml_home",
                    confidence=GRADE_A,
                    qualifier_flags=ml_home_flags,
                    odds=home_odds,
                ))
            elif is_near_miss(home_prob, ORIG_HOME_ML_PROB):
                near_misses.append({
                    "market": "ml_home",
                    "flags":  ml_home_flags,
                })

        if away_odds:
            away_prob = odds_to_implied_prob(away_odds)
            ml_away_flags = {
                "implied_prob": away_prob,
                "threshold":    ORIG_AWAY_ML_PROB,
                "odds":         away_odds,
                "pass":         away_prob >= ORIG_AWAY_ML_PROB,
            }
            if ml_away_flags["pass"]:
                qualified.append(QualifiedMarket(
                    market="ml_away",
                    confidence=GRADE_A,
                    qualifier_flags=ml_away_flags,
                    odds=away_odds,
                ))
            elif is_near_miss(away_prob, ORIG_AWAY_ML_PROB):
                near_misses.append({
                    "market": "ml_away",
                    "flags":  ml_away_flags,
                })

    return qualified, near_misses


# ─────────────────────────────────────────
# STAGE 3 — FORM MODIFIER
# ─────────────────────────────────────────

def _stage3_form(
    home_form: list[dict],
    away_form: list[dict],
) -> str:
    """
    Evaluate recent form for both teams.
    Returns GRADE_A if form is strong, GRADE_B if any check fails.
    Does NOT void the pick — only downgrades confidence.
    """
    if not home_form or not away_form:
        return GRADE_B   # missing form data = conservative downgrade

    # Check 1 — home team won last home game
    last_home = home_form[0] if home_form else None
    if last_home and last_home.get("lost"):
        logger.info("Form downgrade — home team lost last home game")
        return GRADE_B

    # Check 2 — home team won 3+ of last 5 home games
    home_wins = sum(1 for g in home_form if g.get("won"))
    if home_wins < ORIG_FORM_HOME_WINS:
        logger.info(
            f"Form downgrade — home wins {home_wins} "
            f"< {ORIG_FORM_HOME_WINS} in last 5"
        )
        return GRADE_B

    # Check 3 — away team lost 3+ of last 5 away games
    away_losses = sum(1 for g in away_form if g.get("lost"))
    if away_losses < ORIG_FORM_AWAY_LOSSES:
        logger.info(
            f"Form downgrade — away losses {away_losses} "
            f"< {ORIG_FORM_AWAY_LOSSES} in last 5"
        )
        return GRADE_B

    # Check 4 — away team lost last away game
    last_away = away_form[0] if away_form else None
    if last_away and not last_away.get("lost"):
        logger.info("Form downgrade — away team did not lose last away game")
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
    Apply final confidence grades to all qualified markets.

    Grade A: all filters pass + odds > 1.50 + form pass
    Grade B: filters pass + form fails OR odds <= 1.50
             OR line moved against pick
    Grade C: odds imply prob > 0.92 (edge priced in)
    """
    from data.odds_fetcher import (
        detect_line_movement,
        assess_odds_grade,
        odds_to_implied_prob,
    )

    graded = []

    for market in qualified:
        grade    = GRADE_A
        reasons  = []

        # Apply form grade
        if form_grade == GRADE_B:
            grade = GRADE_B
            reasons.append("form_downgrade")

        # Apply odds checks if available
        if market.odds:
            implied_prob = odds_to_implied_prob(market.odds)
            odds_grade   = assess_odds_grade(market.odds, implied_prob)

            if odds_grade == GRADE_C:
                grade = GRADE_C
                reasons.append("edge_priced_in")
            elif odds_grade == GRADE_B and grade == GRADE_A:
                grade = GRADE_B
                reasons.append("odds_below_threshold")

            # Line movement check
            if market.opening_odds and market.odds:
                movement = detect_line_movement(
                    market.opening_odds,
                    market.odds,
                )
                if movement["against_pick"]:
                    if grade == GRADE_A:
                        grade = GRADE_B
                    reasons.append(
                        f"line_moved_{movement['movement_pct']}pct"
                    )

        market.confidence   = grade
        market.grade_reasons = reasons
        graded.append(market)

    return graded


# ─────────────────────────────────────────
# HVLO MODE — SEPARATE PIPELINE
# ─────────────────────────────────────────

def _evaluate_hvlo(
    fixture:    dict,
    home_stats: dict,
    away_stats: dict,
    home_form:  list[dict],
    away_form:  list[dict],
    odds_data:  Optional[dict],
    min_games:  int,
) -> FilterResult:
    """
    HVLO mode evaluation pipeline.
    Simpler gate — no standings requirement, no derby check.
    Targets Over 1.5, BTTS, Over 2.5, Home W/D accumulators.
    Each leg must have odds between 1.10 and 1.40.
    Target acca odds between 2.00 and 3.50.
    """
    fid = fixture["fixture_id"]

    # ── HVLO STAGE 1 — MINIMAL GATE ──────────────────────────────
    home_played = home_stats.get("games_played_total", 0)
    away_played = away_stats.get("games_played_total", 0)

    if home_played < min_games:
        return FilterResult(
            fixture_id=fid,
            mode=MODE_HVLO,
            passed=False,
            void_reason=f"home_insufficient_games:{home_played}",
            void_stage=1,
        )
    if away_played < min_games:
        return FilterResult(
            fixture_id=fid,
            mode=MODE_HVLO,
            passed=False,
            void_reason=f"away_insufficient_games:{away_played}",
            void_stage=1,
        )

    # ── HVLO STAGE 2 — MARKET CHECKS ─────────────────────────────
    qualified   = []
    near_misses = []

    def is_near_miss(value: float, threshold: float) -> bool:
        if threshold == 0:
            return False
        return abs(value - threshold) / threshold <= NEAR_MISS_TOLERANCE

    # BTTS check
    home_score_rate = home_stats.get("home_scoring_rate", 0)
    away_score_rate = away_stats.get("away_scoring_rate", 0)
    btts_rate       = (home_score_rate + away_score_rate) / 2

    btts_flags = {
        "combined_score_rate": btts_rate,
        "threshold":           HVLO_BTTS_HIT_RATE,
        "pass":                btts_rate >= HVLO_BTTS_HIT_RATE,
    }

    if btts_flags["pass"]:
        qualified.append(QualifiedMarket(
            market="btts",
            confidence=GRADE_A,
            qualifier_flags=btts_flags,
        ))
    elif is_near_miss(btts_rate, HVLO_BTTS_HIT_RATE):
        near_misses.append({"market": "btts", "flags": btts_flags})

    # Over 1.5 check
    total_avg_goals = (
        home_stats.get("avg_goals_scored_home", 0) +
        away_stats.get("avg_goals_scored_away", 0)
    )
    o15_rate = home_stats.get("over_1_5_rate", 0)

    o15_flags = {
        "total_avg_goals": total_avg_goals,
        "o15_rate":        o15_rate,
        "threshold":       HVLO_OVER_15_RATE,
        "pass":            o15_rate >= HVLO_OVER_15_RATE,
    }

    if o15_flags["pass"]:
        qualified.append(QualifiedMarket(
            market="over_1.5",
            confidence=GRADE_A,
            qualifier_flags=o15_flags,
        ))
    elif is_near_miss(o15_rate, HVLO_OVER_15_RATE):
        near_misses.append({"market": "over_1.5", "flags": o15_flags})

    # Over 2.5 check
    o25_rate = home_stats.get("over_2_5_rate", 0)

    o25_flags = {
        "o25_rate":  o25_rate,
        "threshold": HVLO_OVER_25_RATE,
        "pass":      o25_rate >= HVLO_OVER_25_RATE,
    }

    if o25_flags["pass"]:
        qualified.append(QualifiedMarket(
            market="over_2.5",
            confidence=GRADE_A,
            qualifier_flags=o25_flags,
        ))
    elif is_near_miss(o25_rate, HVLO_OVER_25_RATE):
        near_misses.append({"market": "over_2.5", "flags": o25_flags})

    # Home Win or Draw
    home_win_draw_rate = home_stats.get("home_win_draw_rate", 0)

    hwd_flags = {
        "home_win_draw_rate": home_win_draw_rate,
        "pass":               home_win_draw_rate >= 0.65,
    }

    if hwd_flags["pass"]:
        qualified.append(QualifiedMarket(
            market="home_win_or_draw",
            confidence=GRADE_A,
            qualifier_flags=hwd_flags,
        ))

    if not qualified:
        return FilterResult(
            fixture_id=fid,
            mode=MODE_HVLO,
            passed=False,
            void_reason="no_hvlo_markets_qualified",
            void_stage=2,
            near_misses=near_misses,
        )

    # ── HVLO STAGE 3 — ODDS GATE ──────────────────────────────────
    # Each leg must be between 1.10 and 1.40
    if odds_data:
        odds_qualified = []
        for market in qualified:
            leg_odds = _get_market_odds(market.market, odds_data)
            if leg_odds is None:
                # No odds available — keep market, no odds check
                odds_qualified.append(market)
                continue
            if HVLO_MIN_LEG_ODDS <= leg_odds <= HVLO_MAX_LEG_ODDS:
                market.odds = leg_odds
                odds_qualified.append(market)
            else:
                logger.info(
                    f"HVLO leg rejected — {market.market} "
                    f"odds {leg_odds} outside "
                    f"{HVLO_MIN_LEG_ODDS}–{HVLO_MAX_LEG_ODDS}"
                )
        qualified = odds_qualified

    # ── HVLO STAGE 4 — ACCA VALIDATION ───────────────────────────
    leg_count = len(qualified)

    if leg_count < HVLO_ACCA_MIN_LEGS:
        return FilterResult(
            fixture_id=fid,
            mode=MODE_HVLO,
            passed=False,
            void_reason=(
                f"insufficient_acca_legs:"
                f"{leg_count}<{HVLO_ACCA_MIN_LEGS}"
            ),
            void_stage=4,
            near_misses=near_misses,
        )

    # Cap at max legs
    qualified = qualified[:HVLO_ACCA_MAX_LEGS]

    return FilterResult(
        fixture_id=fid,
        mode=MODE_HVLO,
        passed=True,
        qualified_markets=qualified,
        near_misses=near_misses,
    )


# ─────────────────────────────────────────
# RESULT SETTLEMENT HELPERS
# Called by scheduler/jobs.py after match ends
# ─────────────────────────────────────────

def settle_market(
    market:     str,
    home_score: int,
    away_score: int,
    ht_home:    Optional[int] = None,
    ht_away:    Optional[int] = None,
) -> str:
    """
    Determine win/loss/void for a market given final scores.
    Returns 'won', 'lost', or 'void'.
    """
    total_goals = home_score + away_score

    if market == "under_1.5_0_20min":
        # Approximated from full-time — ideally use live data
        return "won" if total_goals <= 1 else "lost"

    elif market == "win_to_nil":
        return (
            "won"
            if home_score > 0 and away_score == 0
            else "lost"
        )

    elif market == "2h_over_0.5":
        return "won" if total_goals >= 1 else "lost"

    elif market == "ah_minus_1.5":
        return (
            "won"
            if home_score - away_score >= 2
            else "lost"
        )

    elif market == "ht_ft":
        if ht_home is None or ht_away is None:
            return "void"
        ht_home_win = ht_home > ht_away
        ft_home_win = home_score > away_score
        return "won" if (ht_home_win and ft_home_win) else "lost"

    elif market == "no_team_3plus":
        return (
            "won"
            if home_score < 3 and away_score < 3
            else "lost"
        )

    elif market == "btts":
        return (
            "won"
            if home_score > 0 and away_score > 0
            else "lost"
        )

    elif market in ("ml_home", "ml_away"):
        if market == "ml_home":
            return "won" if home_score > away_score else "lost"
        else:
            return "won" if away_score > home_score else "lost"

    elif market in ("over_1.5", "over_2.5", "home_win_or_draw"):
        if market == "over_1.5":
            return "won" if total_goals >= 2 else "lost"
        elif market == "over_2.5":
            return "won" if total_goals >= 3 else "lost"
        else:
            return (
                "won"
                if home_score >= away_score
                else "lost"
            )

    return "void"


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def _void(reason: str) -> dict:
    """Return a standardized void dict for Stage 1 failures."""
    return {"passed": False, "reason": reason}


def _get_position(standings: list[dict], team_id: int) -> Optional[int]:
    """Find a team's position in standings by team ID."""
    for entry in standings:
        if entry.get("team_id") == team_id:
            return entry["position"]
    return None


def _get_market_odds(market: str, odds_data: dict) -> Optional[float]:
    """
    Extract relevant odds for a market from odds_data.
    Used by HVLO leg odds check.
    """
    from data.odds_fetcher import get_over_odds, get_btts_yes_odds
    try:
        if market == "btts":
            return get_btts_yes_odds(odds_data)
        elif market == "over_1.5":
            return get_over_odds(odds_data, 1.5)
        elif market == "over_2.5":
            return get_over_odds(odds_data, 2.5)
        elif market == "home_win_or_draw":
            markets  = odds_data.get("markets", {})
            h2h      = markets.get("h2h", {})
            home     = odds_data.get("home_team", "")
            draw     = h2h.get("Draw", 0)
            home_win = h2h.get(home, 0)
            if home_win and draw:
                # Combined W/D implied prob → converted back to odds
                prob = (1 / home_win) + (1 / draw)
                return round(1 / prob, 4) if prob > 0 else None
        return None
    except Exception:
        return None