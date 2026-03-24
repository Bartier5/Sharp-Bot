import aiohttp
import logging
from datetime import datetime
from config import (
    ODDS_API_BASE,
    ODDS_API_KEY,
    REQUEST_TIMEOUT,
    ORIG_LINE_MOVE_THRESHOLD,
    ORIG_EDGE_CEILING,
    GRADE_A_MIN_ODDS,
    GRADE_B,
    GRADE_C,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# SUPPORTED MARKETS
# Maps internal market names → Odds API market keys
# ─────────────────────────────────────────

FOOTBALL_MARKET_MAP = {
    "ml":             "h2h",
    "win_to_nil":     "h2h",
    "btts":           "btts",
    "over_1.5":       "totals",
    "over_2.5":       "totals",
    "ah_minus_1.5":   "spreads",
    "no_team_3plus":  "totals",
    "ht_ft":          "h2h",
    "2h_over_0.5":    "totals",
}

NBA_MARKET_MAP = {
    "ml":               "h2h",
    "spread":           "spreads",
    "1h_ml":            "h2h_1st_half",
    "team_total_over":  "team_totals",
    "parlay_leg":       "h2h",
}

# Regions to fetch odds from
REGIONS = "eu,uk"

# Odds format — decimal for all calculations
ODDS_FORMAT = "decimal"


# ─────────────────────────────────────────
# BASE REQUEST
# ─────────────────────────────────────────

async def _get(session: aiohttp.ClientSession,
               endpoint: str,
               params: dict) -> dict | list | None:
    """
    Central request handler for The Odds API.
    Returns parsed JSON or None on failure.
    """
    url = f"{ODDS_API_BASE}/{endpoint}"
    params["apiKey"] = ODDS_API_KEY

    try:
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        ) as response:
            # Log remaining quota on every call
            remaining = response.headers.get(
                "x-requests-remaining", "unknown"
            )
            used = response.headers.get(
                "x-requests-used", "unknown"
            )
            logger.info(
                f"Odds API quota — used: {used}, remaining: {remaining}"
            )

            if response.status == 401:
                logger.error("Odds API — invalid API key")
                return None
            if response.status == 429:
                logger.error("Odds API — quota exceeded")
                return None
            if response.status != 200:
                logger.error(
                    f"Odds API {endpoint} returned {response.status}"
                )
                return None

            return await response.json()

    except aiohttp.ClientTimeout:
        logger.error(f"Timeout on Odds API {endpoint}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error on Odds API {endpoint}: {e}")
        return None


# ─────────────────────────────────────────
# SPORT KEYS
# The Odds API uses sport keys not IDs
# ─────────────────────────────────────────

FOOTBALL_SPORT_KEYS = {
    179: "soccer_scotland_premiership",
    144: "soccer_belgium_first_div",
    88:  "soccer_netherlands_eredivisie",
    197: "soccer_greece_super_league",
    271: "soccer_hungary_nemzeti_bajnoksag",
    218: "soccer_austria_bundesliga",
    207: "soccer_switzerland_superleague",
    119: "soccer_denmark_superliga",
    103: "soccer_norway_eliteserien",
    94:  "soccer_portugal_primeira_liga",
    107: "soccer_norway_toppserien",
    108: "soccer_norway_toppserien",
    570: "soccer_sweden_allsvenskan",
    253: "soccer_england_league1",
}

NBA_SPORT_KEY = "basketball_nba"


# ─────────────────────────────────────────
# FETCH ODDS FOR A FIXTURE
# ─────────────────────────────────────────

async def get_fixture_odds(session: aiohttp.ClientSession,
                            league_id: int,
                            home_team: str,
                            away_team: str,
                            sport: str = "football") -> dict | None:
    """
    Fetch current odds for a specific fixture.
    Matches fixture by team names since Odds API has no fixture IDs.
    Returns normalized odds dict with all available markets.
    """
    if sport == "football":
        sport_key = FOOTBALL_SPORT_KEYS.get(league_id)
        if not sport_key:
            logger.warning(
                f"No Odds API sport key for league ID {league_id}"
            )
            return None
    else:
        sport_key = NBA_SPORT_KEY

    data = await _get(session, f"sports/{sport_key}/odds", {
        "regions":     REGIONS,
        "markets":     "h2h,spreads,totals",
        "oddsFormat":  ODDS_FORMAT,
        "dateFormat":  "iso",
    })

    if not data:
        return None

    # Match fixture by team names — fuzzy match handles
    # minor name differences between APIs
    matched = _match_fixture(data, home_team, away_team)
    if not matched:
        logger.warning(
            f"Could not match {home_team} vs {away_team} "
            f"in Odds API response"
        )
        return None

    return _normalize_odds(matched)


def _match_fixture(events: list,
                   home_team: str,
                   away_team: str) -> dict | None:
    """
    Find the matching event in Odds API response by team names.
    Uses lowercase partial matching to handle name differences
    between API-Football and The Odds API.
    """
    home_lower = home_team.lower()
    away_lower = away_team.lower()

    for event in events:
        api_home = event.get("home_team", "").lower()
        api_away = event.get("away_team", "").lower()

        # Exact match first
        if api_home == home_lower and api_away == away_lower:
            return event

        # Partial match — handles "Man United" vs "Manchester United"
        if (home_lower[:5] in api_home or api_home[:5] in home_lower) and \
           (away_lower[:5] in api_away or api_away[:5] in away_lower):
            return event

    return None


def _normalize_odds(event: dict) -> dict:
    """
    Extract best available odds per market from all bookmakers.
    Returns dict with market keys mapped to best decimal odds.
    """
    markets = {}

    for bookmaker in event.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            key      = market["key"]
            outcomes = market.get("outcomes", [])

            if key not in markets:
                markets[key] = {}

            for outcome in outcomes:
                name  = outcome["name"]
                price = outcome["price"]

                # Keep best (highest) odds per outcome across bookmakers
                if name not in markets[key] or \
                   price > markets[key][name]:
                    markets[key][name] = price

    return {
        "event_id":   event.get("id"),
        "home_team":  event.get("home_team"),
        "away_team":  event.get("away_team"),
        "commence":   event.get("commence_time"),
        "markets":    markets,
        "fetched_at": datetime.utcnow().isoformat(),
    }


# ─────────────────────────────────────────
# EXTRACT SPECIFIC MARKET ODDS
# ─────────────────────────────────────────

def get_home_win_odds(odds_data: dict) -> float | None:
    """Extract best home win (h2h) odds."""
    try:
        return odds_data["markets"]["h2h"].get(
            odds_data["home_team"]
        )
    except (KeyError, TypeError):
        return None


def get_away_win_odds(odds_data: dict) -> float | None:
    """Extract best away win (h2h) odds."""
    try:
        return odds_data["markets"]["h2h"].get(
            odds_data["away_team"]
        )
    except (KeyError, TypeError):
        return None


def get_btts_yes_odds(odds_data: dict) -> float | None:
    """Extract BTTS Yes odds."""
    try:
        return odds_data["markets"]["btts"].get("Yes")
    except (KeyError, TypeError):
        return None


def get_over_odds(odds_data: dict, line: float) -> float | None:
    """
    Extract Over odds for a specific line (1.5, 2.5 etc).
    Odds API returns outcomes as 'Over 1.5', 'Over 2.5'.
    """
    try:
        key = f"Over {line}"
        return odds_data["markets"]["totals"].get(key)
    except (KeyError, TypeError):
        return None


def get_spread_odds(odds_data: dict,
                    team_name: str) -> tuple[float, float] | None:
    """
    Extract spread line and odds for a specific team.
    Returns (spread_line, odds) tuple or None.
    """
    try:
        spreads = odds_data["markets"]["spreads"]
        if team_name in spreads:
            return spreads[team_name]
        return None
    except (KeyError, TypeError):
        return None


def get_nba_team_total(odds_data: dict,
                        team_name: str) -> dict | None:
    """
    Extract team total over/under for NBA team total market.
    Returns {"over": odds, "under": odds, "line": N} or None.
    """
    try:
        totals = odds_data["markets"].get("team_totals", {})
        return totals.get(team_name)
    except (KeyError, TypeError):
        return None


# ─────────────────────────────────────────
# IMPLIED PROBABILITY
# ─────────────────────────────────────────

def odds_to_implied_prob(decimal_odds: float) -> float:
    """
    Convert decimal odds to implied probability.
    1.50 odds → 0.667 (66.7% implied probability)
    Rounds to 4 decimal places.
    """
    if not decimal_odds or decimal_odds <= 1.0:
        return 0.0
    return round(1 / decimal_odds, 4)


def implied_prob_to_odds(probability: float) -> float:
    """
    Convert probability to fair decimal odds.
    0.80 probability → 1.25 fair odds.
    """
    if not probability or probability <= 0:
        return 0.0
    return round(1 / probability, 4)


def remove_overround(odds_list: list[float]) -> list[float]:
    """
    Remove bookmaker margin from a set of odds.
    Returns true probability-based odds.
    Example: [1.80, 2.10] → adjusted to sum to 100% implied prob.
    """
    if not odds_list:
        return []
    probs     = [1 / o for o in odds_list if o > 1.0]
    total_prob = sum(probs)
    if total_prob == 0:
        return odds_list
    # True probabilities sum to 1.0
    true_probs = [p / total_prob for p in probs]
    return [round(1 / p, 4) for p in true_probs]


# ─────────────────────────────────────────
# LINE MOVEMENT DETECTION
# ─────────────────────────────────────────

def detect_line_movement(opening_odds: float,
                          current_odds: float,
                          pick_direction: str = "home") -> dict:
    """
    Compare opening odds to current odds and detect sharp movement.

    pick_direction: 'home' or 'away'
    Movement is measured as percentage change from opening.

    Negative movement = odds drifted OUT (pick became less favoured)
    Positive movement = odds shortened IN (pick became more favoured)

    Returns dict with movement data and grade impact.
    """
    if not opening_odds or not current_odds:
        return {
            "movement_pct":   0.0,
            "direction":      "unknown",
            "against_pick":   False,
            "grade_impact":   None,
        }

    movement_pct = (current_odds - opening_odds) / opening_odds

    # Odds drifting OUT means market moved against your pick
    # e.g. opening 1.80 → current 2.10 = +16.7% drift = against pick
    against_pick = movement_pct > ORIG_LINE_MOVE_THRESHOLD

    return {
        "movement_pct": round(movement_pct * 100, 2),
        "direction":    "out" if movement_pct > 0 else "in",
        "against_pick": against_pick,
        "grade_impact": GRADE_B if against_pick else None,
    }


def store_opening_odds(odds_data: dict) -> dict:
    """
    Extract current odds to use as opening odds reference.
    Called on first odds fetch for a fixture.
    Returns flat dict of market → outcome → odds.
    Stored in picks.opening_odds as JSON.
    """
    snapshot = {}
    for market_key, outcomes in odds_data.get("markets", {}).items():
        snapshot[market_key] = dict(outcomes)
    return snapshot


# ─────────────────────────────────────────
# GRADE IMPACT FROM ODDS
# ─────────────────────────────────────────

def assess_odds_grade(odds: float,
                       implied_prob: float) -> str | None:
    """
    Assess whether the odds quality affects the confidence grade.

    Rules:
    - odds <= GRADE_A_MIN_ODDS (1.50) → Grade B (no value)
    - implied_prob > ORIG_EDGE_CEILING (0.92) → Grade C (priced in)
    - otherwise → no grade impact

    Returns grade string or None if no impact.
    """
    if not odds:
        return None

    if implied_prob > ORIG_EDGE_CEILING:
        return GRADE_C          # edge already priced in

    if odds <= GRADE_A_MIN_ODDS:
        return GRADE_B          # below value threshold

    return None                 # no grade impact


# ─────────────────────────────────────────
# BATCH ODDS FETCH (quota-aware)
# ─────────────────────────────────────────

async def get_odds_for_picks(session: aiohttp.ClientSession,
                              picks: list[dict]) -> dict:
    """
    Fetch odds for a list of qualified picks.
    Groups by sport key to minimize API calls —
    one call per sport key covers all fixtures in that league.

    Returns dict mapping fixture_id → odds_data.
    """
    # Group picks by sport key to batch requests
    by_sport_key: dict[str, list] = {}

    for pick in picks:
        league_id = pick.get("league_id")
        sport     = pick.get("sport", "football")

        if sport == "football":
            sport_key = FOOTBALL_SPORT_KEYS.get(league_id)
        else:
            sport_key = NBA_SPORT_KEY

        if not sport_key:
            continue

        if sport_key not in by_sport_key:
            by_sport_key[sport_key] = []
        by_sport_key[sport_key].append(pick)

    results = {}

    for sport_key, group_picks in by_sport_key.items():
        # One API call fetches ALL fixtures for this sport/league
        data = await _get(session, f"sports/{sport_key}/odds", {
            "regions":    REGIONS,
            "markets":    "h2h,spreads,totals",
            "oddsFormat": ODDS_FORMAT,
            "dateFormat": "iso",
        })

        if not data:
            continue

        # Match each pick to its event in the response
        for pick in group_picks:
            matched = _match_fixture(
                data,
                pick["home_team"],
                pick["away_team"]
            )
            if matched:
                results[pick["fixture_id"]] = _normalize_odds(matched)

    return results