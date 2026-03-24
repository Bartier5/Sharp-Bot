import aiohttp
import logging
from datetime import datetime, timedelta
from config import (
    API_FOOTBALL_BASE,
    API_FOOTBALL_KEY,
    ORIGINAL_LEAGUES,
    HVLO_LEAGUES,
    LEAGUE_MODE_MAP,
    MODE_ORIGINAL,
    MODE_HVLO,
    REQUEST_TIMEOUT,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# HEADERS — sent with every request
# ─────────────────────────────────────────

HEADERS = {
    "x-rapidapi-host": "v3.football.api-sports.io",
    "x-rapidapi-key": API_FOOTBALL_KEY,
}


# ─────────────────────────────────────────
# BASE REQUEST FUNCTION
# ─────────────────────────────────────────

async def _get(session: aiohttp.ClientSession,
               endpoint: str,
               params: dict) -> dict | None:
    """
    Central request function. Every fetcher calls this.
    Handles errors, timeouts, and bad status codes in one place.
    Returns the parsed JSON response dict or None on failure.
    """
    url = f"{API_FOOTBALL_BASE}/{endpoint}"
    try:
        async with session.get(
            url,
            headers=HEADERS,
            params=params,
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        ) as response:
            if response.status != 200:
                logger.error(
                    f"API-Football {endpoint} returned {response.status}"
                )
                return None
            data = await response.json()

            # API-Football wraps everything in a 'response' key
            # errors come back as 200 with an 'errors' dict
            if data.get("errors"):
                logger.error(
                    f"API-Football error on {endpoint}: {data['errors']}"
                )
                return None

            return data

    except aiohttp.ClientTimeout:
        logger.error(f"Timeout on {endpoint}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error on {endpoint}: {e}")
        return None


# ─────────────────────────────────────────
# SESSION FACTORY
# ─────────────────────────────────────────

def make_session() -> aiohttp.ClientSession:
    """Create a reusable aiohttp session for a batch of requests."""
    return aiohttp.ClientSession()


# ─────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────

async def get_todays_fixtures(session: aiohttp.ClientSession) -> list[dict]:
    """
    Fetch all fixtures for today across every monitored league.
    Returns a flat list of normalized fixture dicts.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    all_leagues = {**ORIGINAL_LEAGUES, **HVLO_LEAGUES}
    fixtures = []

    for league_name, league_id in all_leagues.items():
        data = await _get(session, "fixtures", {
            "league": league_id,
            "date":   today,
            "season": _current_season(),
        })

        if not data or not data.get("response"):
            logger.info(f"No fixtures today for {league_name}")
            continue

        for item in data["response"]:
            fixture = _normalize_fixture(item, league_name, league_id)
            if fixture:
                fixtures.append(fixture)

    logger.info(f"Fetched {len(fixtures)} fixtures for {today}")
    return fixtures


async def get_fixture_by_id(session: aiohttp.ClientSession,
                             fixture_id: int) -> dict | None:
    """Fetch a single fixture by its API-Football ID."""
    data = await _get(session, "fixtures", {"id": fixture_id})
    if not data or not data.get("response"):
        return None
    item = data["response"][0]
    league_id = item["league"]["id"]
    league_name = item["league"]["name"]
    return _normalize_fixture(item, league_name, league_id)


def _normalize_fixture(item: dict,
                        league_name: str,
                        league_id: int) -> dict | None:
    """
    Convert raw API-Football fixture response into a clean dict.
    Returns None if the fixture is missing critical fields.
    """
    try:
        fix      = item["fixture"]
        teams    = item["teams"]
        goals    = item.get("goals", {})
        mode     = LEAGUE_MODE_MAP.get(league_id, MODE_ORIGINAL)

        return {
            "fixture_id": str(fix["id"]),
            "sport":      "football",
            "mode":       mode,
            "league":     league_name,
            "league_id":  league_id,
            "home_team":  teams["home"]["name"],
            "home_id":    teams["home"]["id"],
            "away_team":  teams["away"]["name"],
            "away_id":    teams["away"]["id"],
            "kickoff":    fix["date"],
            "status":     fix["status"]["short"],
            "home_score": goals.get("home"),
            "away_score": goals.get("away"),
            "venue":      fix.get("venue", {}).get("name"),
        }
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to normalize fixture: {e}")
        return None


# ─────────────────────────────────────────
# STANDINGS
# ─────────────────────────────────────────

async def get_standings(session: aiohttp.ClientSession,
                         league_id: int) -> list[dict] | None:
    """
    Fetch current league standings.
    Returns a list of team standing dicts ordered by position.
    """
    data = await _get(session, "standings", {
        "league": league_id,
        "season": _current_season(),
    })

    if not data or not data.get("response"):
        return None

    try:
        standings_raw = (
            data["response"][0]["league"]["standings"][0]
        )
        standings = []
        for entry in standings_raw:
            standings.append({
                "team_id":       entry["team"]["id"],
                "team_name":     entry["team"]["name"],
                "position":      entry["rank"],
                "played":        entry["all"]["played"],
                "wins":          entry["all"]["win"],
                "draws":         entry["all"]["draw"],
                "losses":        entry["all"]["lose"],
                "goals_for":     entry["all"]["goals"]["for"],
                "goals_against": entry["all"]["goals"]["against"],
                "points":        entry["points"],
                "form":          entry.get("form", ""),
            })
        return standings
    except (KeyError, IndexError) as e:
        logger.error(f"Failed to parse standings for league {league_id}: {e}")
        return None


def get_team_position(standings: list[dict], team_id: int) -> int | None:
    """Extract a team's league position from standings list."""
    for entry in standings:
        if entry["team_id"] == team_id:
            return entry["position"]
    return None


def get_league_size(standings: list[dict]) -> int:
    """Return total number of teams in the league."""
    return len(standings)


# ─────────────────────────────────────────
# TEAM STATISTICS
# ─────────────────────────────────────────

async def get_team_stats(session: aiohttp.ClientSession,
                          team_id: int,
                          league_id: int) -> dict | None:
    """
    Fetch season statistics for a team in a specific league.
    Returns a normalized stats dict with all SHARP filter fields.
    """
    data = await _get(session, "teams/statistics", {
        "team":   team_id,
        "league": league_id,
        "season": _current_season(),
    })

    if not data or not data.get("response"):
        return None

    try:
        r     = data["response"]
        games = r["fixtures"]
        goals = r["goals"]
        cs    = r.get("clean_sheet", {})
        ftg   = r.get("failed_to_score", {})

        home_played = games["played"].get("home", 0)
        away_played = games["played"].get("away", 0)

        # Goals scored
        home_scored = goals["for"]["total"].get("home", 0)
        away_scored = goals["for"]["total"].get("away", 0)

        # Goals conceded
        home_conceded = goals["against"]["total"].get("home", 0)
        away_conceded = goals["against"]["total"].get("away", 0)

        # Clean sheets
        home_cs = cs.get("home", 0)
        away_cs = cs.get("away", 0)

        # Wins
        home_wins = games["wins"].get("home", 0)
        away_wins = games["wins"].get("away", 0)

        return {
            "team_id":            team_id,
            "games_played_home":  home_played,
            "games_played_away":  away_played,
            "games_played_total": home_played + away_played,

            # Per-game averages
            "avg_goals_scored_home":    _safe_div(home_scored, home_played),
            "avg_goals_scored_away":    _safe_div(away_scored, away_played),
            "avg_goals_conceded_home":  _safe_div(home_conceded, home_played),
            "avg_goals_conceded_away":  _safe_div(away_conceded, away_played),

            # Rates (0.0 – 1.0)
            "cs_rate_home":  _safe_div(home_cs, home_played),
            "cs_rate_away":  _safe_div(away_cs, away_played),
            "win_rate_home": _safe_div(home_wins, home_played),
            "win_rate_away": _safe_div(away_wins, away_played),

            # Raw counts for win-by-2 calculation
            "home_played": home_played,
            "away_played": away_played,
        }
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to parse team stats for team {team_id}: {e}")
        return None


# ─────────────────────────────────────────
# LAST N FIXTURES (FORM)
# ─────────────────────────────────────────

async def get_last_fixtures(session: aiohttp.ClientSession,
                             team_id: int,
                             count: int = 5,
                             venue: str = "home") -> list[dict] | None:
    """
    Fetch last N home or away fixtures for a team.
    venue: 'home' or 'away'
    Returns list of normalized result dicts ordered most-recent first.
    """
    # Fetch last 20 to ensure we get enough home/away games
    data = await _get(session, "fixtures", {
        "team":   team_id,
        "last":   20,
        "status": "FT",
    })

    if not data or not data.get("response"):
        return None

    results = []
    for item in data["response"]:
        teams     = item["teams"]
        goals     = item["goals"]
        is_home   = teams["home"]["id"] == team_id

        if venue == "home" and not is_home:
            continue
        if venue == "away" and is_home:
            continue

        home_goals = goals.get("home", 0) or 0
        away_goals = goals.get("away", 0) or 0

        if is_home:
            scored    = home_goals
            conceded  = away_goals
            won       = home_goals > away_goals
            lost      = home_goals < away_goals
        else:
            scored    = away_goals
            conceded  = home_goals
            won       = away_goals > home_goals
            lost      = away_goals < home_goals

        results.append({
            "fixture_id":  str(item["fixture"]["id"]),
            "date":        item["fixture"]["date"],
            "scored":      scored,
            "conceded":    conceded,
            "won":         won,
            "lost":        lost,
            "draw":        home_goals == away_goals,
            "clean_sheet": conceded == 0,
            "scored_2h":   None,   # HT data requires separate endpoint
        })

        if len(results) >= count:
            break

    return results


# ─────────────────────────────────────────
# WIN BY 2+ CALCULATION
# ─────────────────────────────────────────

async def get_win_by_2_rate(session: aiohttp.ClientSession,
                             team_id: int,
                             league_id: int,
                             last_n: int = 10) -> float:
    """
    Calculate rate of home wins by 2+ goals vs bottom-half opponents.
    Uses last N home fixtures. Returns float 0.0–1.0.
    """
    data = await _get(session, "fixtures", {
        "team":   team_id,
        "last":   last_n * 2,   # fetch extra to filter home only
        "status": "FT",
        "league": league_id,
    })

    if not data or not data.get("response"):
        return 0.0

    wins_by_2 = 0
    home_games = 0

    for item in data["response"]:
        teams      = item["teams"]
        goals      = item["goals"]
        is_home    = teams["home"]["id"] == team_id

        if not is_home:
            continue

        home_goals = goals.get("home", 0) or 0
        away_goals = goals.get("away", 0) or 0
        home_games += 1

        if home_goals - away_goals >= 2:
            wins_by_2 += 1

        if home_games >= last_n:
            break

    return _safe_div(wins_by_2, home_games)


# ─────────────────────────────────────────
# INJURIES
# ─────────────────────────────────────────

async def get_injuries(session: aiohttp.ClientSession,
                        fixture_id: int) -> dict:
    """
    Fetch injury/suspension list for a fixture.
    Returns {"home": [...], "away": [...]} with player details.
    """
    data = await _get(session, "injuries", {"fixture": fixture_id})

    result = {"home": [], "away": []}

    if not data or not data.get("response"):
        return result

    for entry in data["response"]:
        side = "home" if entry["team"]["id"] else "away"

        # Determine side from fixture context
        player_info = {
            "name":     entry["player"]["name"],
            "position": entry["player"]["type"],   # Goalkeeper/Defender etc
            "reason":   entry["player"]["reason"],  # Injured/Suspended
            "team_id":  entry["team"]["id"],
        }

        # API returns team ID — we check against home/away in filter
        result["home" if entry["team"].get(
            "id") == entry["team"]["id"] else "away"].append(player_info)

    return result


async def get_injuries_by_team(session: aiohttp.ClientSession,
                                team_id: int,
                                fixture_id: int) -> list[dict]:
    """
    Fetch injuries for a specific team in a specific fixture.
    Returns list of injured/suspended player dicts.
    """
    data = await _get(session, "injuries", {"fixture": fixture_id})

    if not data or not data.get("response"):
        return []

    return [
        {
            "name":     e["player"]["name"],
            "position": e["player"]["type"],
            "reason":   e["player"]["reason"],
        }
        for e in data["response"]
        if e["team"]["id"] == team_id
    ]


# ─────────────────────────────────────────
# REFEREE STATISTICS
# ─────────────────────────────────────────

async def get_referee_stats(session: aiohttp.ClientSession,
                             fixture_id: int) -> dict | None:
    """
    Fetch referee details for a fixture.
    Returns referee name and average cards/game if available.
    Note: free tier data is sparse for smaller leagues — returns None
    gracefully if unavailable.
    """
    data = await _get(session, "fixtures", {"id": fixture_id})

    if not data or not data.get("response"):
        return None

    try:
        fix      = data["response"][0]["fixture"]
        referee  = fix.get("referee")
        if not referee:
            return None

        return {
            "name":          referee,
            "avg_cards":     None,   # not available on free tier directly
            "cards_warning": None,   # populated from fixture events if needed
        }
    except (KeyError, IndexError):
        return None


async def get_fixture_events(session: aiohttp.ClientSession,
                              fixture_id: int) -> list[dict]:
    """
    Fetch all events for a fixture (goals, cards, subs).
    Used to count yellow cards for referee chaos profiling.
    """
    data = await _get(session, "fixtures/events", {
        "fixture": fixture_id,
        "type":    "Card",
    })

    if not data or not data.get("response"):
        return []

    return [
        {
            "minute": e["time"]["elapsed"],
            "type":   e["detail"],        # Yellow Card / Red Card
            "team":   e["team"]["name"],
            "player": e["player"]["name"],
        }
        for e in data["response"]
    ]


# ─────────────────────────────────────────
# LEAGUE STATISTICS (for ORIG U1.5 filter)
# ─────────────────────────────────────────

async def get_league_avg_goals(session: aiohttp.ClientSession,
                                league_id: int) -> float:
    """
    Calculate average goals per game for a league this season.
    Used for the Under 1.5 Goals 0-20min filter.
    """
    data = await _get(session, "fixtures", {
        "league": league_id,
        "season": _current_season(),
        "status": "FT",
        "last":   50,    # last 50 finished fixtures
    })

    if not data or not data.get("response"):
        return 0.0

    total_goals  = 0
    total_games  = 0

    for item in data["response"]:
        goals = item.get("goals", {})
        home  = goals.get("home", 0) or 0
        away  = goals.get("away", 0) or 0
        total_goals += home + away
        total_games += 1

    return _safe_div(total_goals, total_games)


# ─────────────────────────────────────────
# FIXTURE RESULT CHECKER (for scheduler)
# ─────────────────────────────────────────

async def get_fixture_result(session: aiohttp.ClientSession,
                              fixture_id: int) -> dict | None:
    """
    Fetch current status and score for a specific fixture.
    Called by the result checker scheduler job.
    """
    data = await _get(session, "fixtures", {"id": fixture_id})

    if not data or not data.get("response"):
        return None

    try:
        item   = data["response"][0]
        fix    = item["fixture"]
        goals  = item.get("goals", {})
        status = fix["status"]["short"]

        return {
            "fixture_id": str(fix["id"]),
            "status":     status,
            "home_score": goals.get("home"),
            "away_score": goals.get("away"),
            "finished":   status in ("FT", "AET", "PEN"),
        }
    except (KeyError, IndexError) as e:
        logger.error(f"Failed to parse fixture result {fixture_id}: {e}")
        return None


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def _current_season() -> int:
    """
    Return current football season year.
    Seasons run Aug–May so Jan–July = previous year's season.
    Example: March 2026 → season 2025
    """
    now = datetime.utcnow()
    return now.year if now.month >= 8 else now.year - 1


def _safe_div(numerator: float, denominator: float) -> float:
    """Safe division — returns 0.0 instead of ZeroDivisionError."""
    if not denominator:
        return 0.0
    return round(numerator / denominator, 4)