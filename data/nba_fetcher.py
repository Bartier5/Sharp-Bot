import aiohttp
import logging
from datetime import datetime
from config import (
    API_FOOTBALL_BASE,
    API_FOOTBALL_KEY,
    REQUEST_TIMEOUT,
    NBA_MIN_GAMES,
    NBA_LOSING_STREAK_MAX,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

NBA_LEAGUE_ID = 12       # API-Football NBA league ID
NBA_SEASON    = 2025     # Current NBA season year

HEADERS = {
    "x-rapidapi-host": "v3.football.api-sports.io",
    "x-rapidapi-key":  API_FOOTBALL_KEY,
}


# ─────────────────────────────────────────
# BASE REQUEST — reuses same pattern as football_fetcher
# ─────────────────────────────────────────

async def _get(session: aiohttp.ClientSession,
               endpoint: str,
               params: dict) -> dict | None:
    """Central request handler for all NBA API calls."""
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
                    f"NBA API {endpoint} returned {response.status}"
                )
                return None
            data = await response.json()
            if data.get("errors"):
                logger.error(
                    f"NBA API error on {endpoint}: {data['errors']}"
                )
                return None
            return data
    except aiohttp.ClientTimeout:
        logger.error(f"Timeout on NBA {endpoint}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error on NBA {endpoint}: {e}")
        return None


# ─────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────

async def get_todays_nba_fixtures(
        session: aiohttp.ClientSession) -> list[dict]:
    """
    Fetch all NBA games scheduled for today.
    Returns normalized fixture dicts ready for storage and filtering.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")

    data = await _get(session, "games", {
        "league": NBA_LEAGUE_ID,
        "season": NBA_SEASON,
        "date":   today,
    })

    if not data or not data.get("response"):
        logger.info(f"No NBA fixtures found for {today}")
        return []

    fixtures = []
    for item in data["response"]:
        fixture = _normalize_nba_fixture(item)
        if fixture:
            fixtures.append(fixture)

    logger.info(f"Fetched {len(fixtures)} NBA fixtures for {today}")
    return fixtures


async def get_nba_fixture_by_id(session: aiohttp.ClientSession,
                                 game_id: int) -> dict | None:
    """Fetch a single NBA game by its API ID."""
    data = await _get(session, "games", {"id": game_id})
    if not data or not data.get("response"):
        return None
    return _normalize_nba_fixture(data["response"][0])


def _normalize_nba_fixture(item: dict) -> dict | None:
    """
    Convert raw API-Football NBA game response into a clean dict.
    NBA uses 'games' endpoint — different field names from football.
    """
    try:
        game   = item["game"]
        teams  = item["teams"]
        scores = item.get("scores", {})
        status = item["status"]

        home_score = scores.get("home", {}).get("total")
        away_score = scores.get("away", {}).get("total")

        return {
            "fixture_id":  str(game["id"]),
            "sport":       "nba",
            "mode":        "nba",
            "league":      "NBA",
            "league_id":   NBA_LEAGUE_ID,
            "home_team":   teams["home"]["name"],
            "home_id":     teams["home"]["id"],
            "away_team":   teams["away"]["name"],
            "away_id":     teams["away"]["id"],
            "kickoff":     game["date"]["start"],
            "status":      status["short"],
            "home_score":  home_score,
            "away_score":  away_score,
            "arena":       game.get("arena", {}).get("name"),
        }
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to normalize NBA fixture: {e}")
        return None


# ─────────────────────────────────────────
# STANDINGS / TEAM RECORDS
# ─────────────────────────────────────────

async def get_nba_standings(
        session: aiohttp.ClientSession) -> list[dict] | None:
    """
    Fetch current NBA standings — both conferences.
    Returns flat list ordered by win percentage descending.
    """
    data = await _get(session, "standings", {
        "league": NBA_LEAGUE_ID,
        "season": NBA_SEASON,
    })

    if not data or not data.get("response"):
        return None

    try:
        standings = []
        for entry in data["response"]:
            team   = entry["team"]
            record = entry.get("records", {})
            conf   = entry.get("conference", {})

            wins   = record.get("wins", 0) or 0
            losses = record.get("losses", 0) or 0
            played = wins + losses

            standings.append({
                "team_id":       team["id"],
                "team_name":     team["name"],
                "conference":    conf.get("name", ""),
                "wins":          wins,
                "losses":        losses,
                "played":        played,
                "win_pct":       _safe_div(wins, played),
                "home_wins":     record.get("home", {}).get("wins", 0),
                "home_losses":   record.get("home", {}).get("losses", 0),
                "away_wins":     record.get("away", {}).get("wins", 0),
                "away_losses":   record.get("away", {}).get("losses", 0),
                "streak":        entry.get("streak", {}).get("count", 0),
                "streak_type":   entry.get("streak", {}).get("type", ""),
            })

        # Sort by win percentage descending
        standings.sort(key=lambda x: x["win_pct"], reverse=True)
        return standings

    except (KeyError, TypeError) as e:
        logger.error(f"Failed to parse NBA standings: {e}")
        return None


def get_team_win_pct(standings: list[dict], team_id: int) -> float:
    """Extract win percentage for a specific team from standings."""
    for entry in standings:
        if entry["team_id"] == team_id:
            return entry["win_pct"]
    return 0.0


def get_team_record(standings: list[dict],
                    team_id: int) -> dict | None:
    """Extract full record dict for a specific team."""
    for entry in standings:
        if entry["team_id"] == team_id:
            return entry
    return None


# ─────────────────────────────────────────
# TEAM STATISTICS
# ─────────────────────────────────────────

async def get_nba_team_stats(session: aiohttp.ClientSession,
                              team_id: int) -> dict | None:
    """
    Fetch season statistics for an NBA team.
    Returns normalized stats dict with all SHARP NBA filter fields.
    """
    data = await _get(session, "teams/statistics", {
        "id":     team_id,
        "league": NBA_LEAGUE_ID,
        "season": NBA_SEASON,
    })

    if not data or not data.get("response"):
        return None

    try:
        r = data["response"]

        # Points scored and conceded
        pts_for     = r.get("points", {}).get("for", {})
        pts_against = r.get("points", {}).get("against", {})

        home_scored    = pts_for.get("average", {}).get("home", 0) or 0
        away_scored    = pts_for.get("average", {}).get("away", 0) or 0
        home_conceded  = pts_against.get("average", {}).get("home", 0) or 0
        away_conceded  = pts_against.get("average", {}).get("away", 0) or 0
        total_scored   = pts_for.get("average", {}).get("all", 0) or 0
        total_conceded = pts_against.get("average", {}).get("all", 0) or 0

        # Games breakdown
        games    = r.get("games", {})
        home_w   = games.get("wins", {}).get("home", {}).get("total", 0) or 0
        home_l   = games.get("loses", {}).get("home", {}).get("total", 0) or 0
        away_w   = games.get("wins", {}).get("away", {}).get("total", 0) or 0
        away_l   = games.get("loses", {}).get("away", {}).get("total", 0) or 0

        home_played = home_w + home_l
        away_played = away_w + away_l
        total_played = home_played + away_played

        return {
            "team_id":             team_id,
            "games_played_total":  total_played,
            "games_played_home":   home_played,
            "games_played_away":   away_played,

            # Scoring averages
            "avg_pts_scored_home":    float(home_scored),
            "avg_pts_scored_away":    float(away_scored),
            "avg_pts_scored_total":   float(total_scored),
            "avg_pts_conceded_home":  float(home_conceded),
            "avg_pts_conceded_away":  float(away_conceded),
            "avg_pts_conceded_total": float(total_conceded),

            # Win rates
            "home_win_rate": _safe_div(home_w, home_played),
            "away_win_rate": _safe_div(away_w, away_played),

            # Winning margin (avg pts scored - avg pts conceded)
            "avg_winning_margin": round(
                float(total_scored) - float(total_conceded), 2
            ),
        }
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to parse NBA team stats {team_id}: {e}")
        return None


# ─────────────────────────────────────────
# OFFENSIVE RATING / LEAGUE RANK
# ─────────────────────────────────────────

async def get_nba_offense_rankings(
        session: aiohttp.ClientSession) -> list[dict]:
    """
    Build offensive ranking for all NBA teams this season.
    Used to check if a team is top-10 offense for team total OVER.
    Returns list ordered by avg points scored descending.
    """
    data = await _get(session, "standings", {
        "league": NBA_LEAGUE_ID,
        "season": NBA_SEASON,
    })

    if not data or not data.get("response"):
        return []

    team_scoring = []
    for entry in data["response"]:
        team   = entry["team"]
        points = entry.get("points", {})
        avg    = points.get("for", {}).get("average", 0) or 0

        team_scoring.append({
            "team_id":   team["id"],
            "team_name": team["name"],
            "avg_pts":   float(avg),
        })

    # Sort by scoring average descending — rank 1 = highest scorer
    team_scoring.sort(key=lambda x: x["avg_pts"], reverse=True)
    for i, team in enumerate(team_scoring):
        team["offense_rank"] = i + 1

    return team_scoring


def get_offense_rank(rankings: list[dict], team_id: int) -> int:
    """Get a team's offensive rank from the pre-built rankings list."""
    for entry in rankings:
        if entry["team_id"] == team_id:
            return entry["offense_rank"]
    return 99   # default — not ranked / not found


# ─────────────────────────────────────────
# LAST N GAMES (FORM + STREAK)
# ─────────────────────────────────────────

async def get_last_nba_games(session: aiohttp.ClientSession,
                              team_id: int,
                              count: int = 5) -> list[dict] | None:
    """
    Fetch last N finished games for an NBA team.
    Returns list of result dicts ordered most-recent first.
    Used for form check and losing streak calculation.
    """
    data = await _get(session, "games", {
        "league": NBA_LEAGUE_ID,
        "season": NBA_SEASON,
        "team":   team_id,
        "last":   count,
    })

    if not data or not data.get("response"):
        return None

    results = []
    for item in data["response"]:
        teams  = item["teams"]
        scores = item.get("scores", {})
        status = item["status"]["short"]

        if status not in ("FT", "AOT"):
            continue

        is_home    = teams["home"]["id"] == team_id
        home_score = scores.get("home", {}).get("total", 0) or 0
        away_score = scores.get("away", {}).get("total", 0) or 0

        if is_home:
            team_score = home_score
            opp_score  = away_score
        else:
            team_score = away_score
            opp_score  = home_score

        won    = team_score > opp_score
        margin = team_score - opp_score

        results.append({
            "game_id":     str(item["game"]["id"]),
            "date":        item["game"]["date"]["start"],
            "is_home":     is_home,
            "team_score":  team_score,
            "opp_score":   opp_score,
            "won":         won,
            "lost":        not won,
            "margin":      margin,
        })

    return results


def calculate_losing_streak(games: list[dict]) -> int:
    """
    Count current consecutive losses from most recent game backwards.
    Returns 0 if the team won their last game.
    """
    streak = 0
    for game in games:       # already ordered most-recent first
        if game["lost"]:
            streak += 1
        else:
            break
    return streak


def calculate_avg_margin(games: list[dict]) -> float:
    """
    Calculate average winning margin across last N games.
    Losses count as negative margins.
    """
    if not games:
        return 0.0
    total = sum(g["margin"] for g in games)
    return round(total / len(games), 2)


# ─────────────────────────────────────────
# PLAYER STATUS (INJURY / QUESTIONABLE)
# ─────────────────────────────────────────

async def get_nba_player_status(session: aiohttp.ClientSession,
                                 team_id: int,
                                 game_id: int) -> list[dict]:
    """
    Fetch player availability for an NBA game.
    Returns list of players flagged as out or questionable.
    Used to check if a top-2 by-minutes player is unavailable.
    """
    data = await _get(session, "injuries", {
        "game":   game_id,
        "team":   team_id,
        "league": NBA_LEAGUE_ID,
        "season": NBA_SEASON,
    })

    if not data or not data.get("response"):
        return []

    return [
        {
            "player_id": e["player"]["id"],
            "name":      e["player"]["name"],
            "status":    e["player"]["status"],   # Out / Questionable
            "reason":    e["player"].get("reason", ""),
        }
        for e in data["response"]
        if e["player"]["status"] in ("Out", "Questionable")
    ]


async def get_top_players_by_minutes(session: aiohttp.ClientSession,
                                      team_id: int) -> list[dict]:
    """
    Fetch top players for a team ranked by average minutes played.
    Returns top 2 — these are the 'star players' the NBA filter protects.
    """
    data = await _get(session, "players/statistics", {
        "team":   team_id,
        "league": NBA_LEAGUE_ID,
        "season": NBA_SEASON,
    })

    if not data or not data.get("response"):
        return []

    players = []
    for entry in data["response"]:
        player = entry.get("player", {})
        stats  = entry.get("statistics", [{}])[0]
        games  = stats.get("games", {})
        mins   = games.get("minutes", "0") or "0"

        # Minutes can come as "MM:SS" string or plain number
        try:
            if ":" in str(mins):
                m, s   = str(mins).split(":")
                avg_min = int(m) + int(s) / 60
            else:
                avg_min = float(mins)
        except (ValueError, TypeError):
            avg_min = 0.0

        players.append({
            "player_id": player.get("id"),
            "name":      player.get("name"),
            "avg_min":   round(avg_min, 2),
        })

    # Sort by minutes descending — top 2 are the star players
    players.sort(key=lambda x: x["avg_min"], reverse=True)
    return players[:2]


# ─────────────────────────────────────────
# RESULT CHECKER (for scheduler)
# ─────────────────────────────────────────

async def get_nba_game_result(session: aiohttp.ClientSession,
                               game_id: int) -> dict | None:
    """
    Fetch current status and score for a specific NBA game.
    Called by the result checker scheduler job.
    """
    data = await _get(session, "games", {"id": game_id})

    if not data or not data.get("response"):
        return None

    try:
        item   = data["response"][0]
        game   = item["game"]
        scores = item.get("scores", {})
        status = item["status"]["short"]

        home_score = scores.get("home", {}).get("total")
        away_score = scores.get("away", {}).get("total")

        return {
            "fixture_id": str(game["id"]),
            "status":     status,
            "home_score": home_score,
            "away_score": away_score,
            "finished":   status in ("FT", "AOT"),
        }
    except (KeyError, IndexError) as e:
        logger.error(f"Failed to parse NBA result {game_id}: {e}")
        return None


# ─────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────

def _safe_div(numerator: float, denominator: float) -> float:
    """Safe division — returns 0.0 instead of ZeroDivisionError."""
    if not denominator:
        return 0.0
    return round(numerator / denominator, 4)