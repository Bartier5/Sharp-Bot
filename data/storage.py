import aiosqlite
import json
import logging
from datetime import datetime
from config import DB_PATH

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# DATABASE INITIALIZATION
# ─────────────────────────────────────────

async def init_db():
    """Create all tables if they don't exist. Called once on bot startup."""
    async with aiosqlite.connect(DB_PATH) as db:

        # ── FIXTURES ──────────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS fixtures (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                fixture_id  TEXT UNIQUE NOT NULL,
                sport       TEXT NOT NULL,
                mode        TEXT NOT NULL,
                league      TEXT NOT NULL,
                league_id   INTEGER,
                home_team   TEXT NOT NULL,
                away_team   TEXT NOT NULL,
                kickoff     DATETIME NOT NULL,
                status      TEXT DEFAULT 'scheduled',
                home_score  INTEGER,
                away_score  INTEGER,
                fetched_at  DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ── PICKS ─────────────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS picks (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                fixture_id       TEXT NOT NULL,
                sport            TEXT NOT NULL,
                mode             TEXT NOT NULL,
                market           TEXT NOT NULL,
                qualifier_flags  TEXT NOT NULL,
                confidence       TEXT NOT NULL,
                odds             REAL,
                opening_odds     REAL,
                result           TEXT DEFAULT 'pending',
                settled_at       DATETIME,
                created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fixture_id) REFERENCES fixtures(fixture_id)
            )
        """)

        # ── BETS ──────────────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS bets (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id          INTEGER NOT NULL,
                pick_id          INTEGER NOT NULL,
                stake            REAL NOT NULL,
                odds             REAL NOT NULL,
                potential_return REAL NOT NULL,
                actual_return    REAL DEFAULT 0,
                status           TEXT DEFAULT 'open',
                placed_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
                settled_at       DATETIME,
                FOREIGN KEY (pick_id) REFERENCES picks(id)
            )
        """)

        # ── USERS ─────────────────────────────────────────────────────
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id             INTEGER PRIMARY KEY,
                username            TEXT,
                sport_preference    TEXT DEFAULT 'both',
                min_games_threshold INTEGER DEFAULT 8,
                first_seen          DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_active         DATETIME,
                total_staked        REAL DEFAULT 0,
                total_returned      REAL DEFAULT 0,
                message_count       INTEGER DEFAULT 0
            )
        """)

        await db.commit()
        logger.info("Database initialized — all tables ready")


# ─────────────────────────────────────────
# USER FUNCTIONS
# ─────────────────────────────────────────

async def upsert_user(user_id: int, username: str):
    """Register new user or update last_active + message_count."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users (user_id, username, last_active, message_count)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(user_id) DO UPDATE SET
                last_active   = excluded.last_active,
                message_count = message_count + 1
        """, (user_id, username, datetime.utcnow()))
        await db.commit()


async def get_user(user_id: int) -> dict | None:
    """Fetch a single user row as a dict."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def update_sport_preference(user_id: int, preference: str):
    """Update user sport preference: 'football', 'nba', or 'both'."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET sport_preference = ? WHERE user_id = ?",
            (preference, user_id)
        )
        await db.commit()


async def update_min_games(user_id: int, threshold: int):
    """Update user's minimum games threshold (8–30)."""
    threshold = max(8, min(30, threshold))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET min_games_threshold = ? WHERE user_id = ?",
            (threshold, user_id)
        )
        await db.commit()


async def get_all_users() -> list[dict]:
    """Fetch all registered users — used by morning briefing scheduler."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users") as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


# ─────────────────────────────────────────
# FIXTURE FUNCTIONS
# ─────────────────────────────────────────

async def upsert_fixture(fixture: dict):
    """Insert or update a fixture row."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO fixtures (
                fixture_id, sport, mode, league, league_id,
                home_team, away_team, kickoff, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fixture_id) DO UPDATE SET
                status     = excluded.status,
                home_score = excluded.home_score,
                away_score = excluded.away_score,
                fetched_at = CURRENT_TIMESTAMP
        """, (
            fixture["fixture_id"],
            fixture["sport"],
            fixture["mode"],
            fixture["league"],
            fixture.get("league_id"),
            fixture["home_team"],
            fixture["away_team"],
            fixture["kickoff"],
            fixture.get("status", "scheduled"),
        ))
        await db.commit()


async def get_fixture(fixture_id: str) -> dict | None:
    """Fetch a single fixture by its API fixture ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM fixtures WHERE fixture_id = ?", (fixture_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def get_todays_fixtures(sport: str = None) -> list[dict]:
    """Fetch all fixtures scheduled for today."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if sport:
            async with db.execute("""
                SELECT * FROM fixtures
                WHERE DATE(kickoff) = ? AND sport = ?
                ORDER BY kickoff ASC
            """, (today, sport)) as cursor:
                rows = await cursor.fetchall()
        else:
            async with db.execute("""
                SELECT * FROM fixtures
                WHERE DATE(kickoff) = ?
                ORDER BY kickoff ASC
            """, (today,)) as cursor:
                rows = await cursor.fetchall()
        return [dict(r) for r in rows]


async def update_fixture_result(fixture_id: str, home_score: int,
                                 away_score: int, status: str):
    """Update final score and status after match ends."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE fixtures
            SET home_score = ?, away_score = ?, status = ?
            WHERE fixture_id = ?
        """, (home_score, away_score, status, fixture_id))
        await db.commit()


async def get_unsettled_fixtures() -> list[dict]:
    """Fetch fixtures that finished but haven't been settled yet."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM fixtures
            WHERE status = 'finished'
            AND fixture_id IN (
                SELECT DISTINCT fixture_id FROM picks
                WHERE result = 'pending'
            )
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


# ─────────────────────────────────────────
# PICK FUNCTIONS
# ─────────────────────────────────────────

async def insert_pick(pick: dict) -> int:
    """Insert a qualified pick. Returns the new pick ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO picks (
                fixture_id, sport, mode, market,
                qualifier_flags, confidence, odds, opening_odds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pick["fixture_id"],
            pick["sport"],
            pick["mode"],
            pick["market"],
            json.dumps(pick["qualifier_flags"]),
            pick["confidence"],
            pick.get("odds"),
            pick.get("opening_odds"),
        ))
        await db.commit()
        return cursor.lastrowid


async def get_pick(pick_id: int) -> dict | None:
    """Fetch a single pick by ID, deserializing qualifier_flags JSON."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM picks WHERE id = ?", (pick_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            pick = dict(row)
            pick["qualifier_flags"] = json.loads(pick["qualifier_flags"])
            return pick


async def get_todays_picks(sport: str = None,
                           mode: str = None) -> list[dict]:
    """Fetch all picks created today, optionally filtered by sport/mode."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        today = datetime.utcnow().strftime("%Y-%m-%d")
        query = "SELECT * FROM picks WHERE DATE(created_at) = ?"
        params = [today]
        if sport:
            query += " AND sport = ?"
            params.append(sport)
        if mode:
            query += " AND mode = ?"
            params.append(mode)
        query += " ORDER BY confidence ASC, odds DESC"
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            result = []
            for r in rows:
                pick = dict(r)
                pick["qualifier_flags"] = json.loads(pick["qualifier_flags"])
                result.append(pick)
            return result


async def update_pick_odds(pick_id: int, new_odds: float):
    """Update current odds on a pick (called by odds refresh job)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE picks SET odds = ? WHERE id = ?",
            (new_odds, pick_id)
        )
        await db.commit()


async def settle_pick(pick_id: int, result: str):
    """Mark a pick as won/lost/void after match settles."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE picks
            SET result = ?, settled_at = ?
            WHERE id = ?
        """, (result, datetime.utcnow(), pick_id))
        await db.commit()


# ─────────────────────────────────────────
# BET FUNCTIONS
# ─────────────────────────────────────────

async def insert_bet(bet: dict) -> int:
    """Log a new bet. Returns the new bet ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("""
            INSERT INTO bets (
                user_id, pick_id, stake, odds, potential_return
            ) VALUES (?, ?, ?, ?, ?)
        """, (
            bet["user_id"],
            bet["pick_id"],
            bet["stake"],
            bet["odds"],
            bet["stake"] * bet["odds"],
        ))
        await db.commit()
        return cursor.lastrowid


async def get_open_bets(user_id: int) -> list[dict]:
    """Fetch all open bets for a user."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT b.*, p.market, p.confidence,
                   f.home_team, f.away_team, f.kickoff
            FROM bets b
            JOIN picks p ON b.pick_id = p.id
            JOIN fixtures f ON p.fixture_id = f.fixture_id
            WHERE b.user_id = ? AND b.status = 'open'
            ORDER BY b.placed_at DESC
        """, (user_id,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_bet_history(user_id: int) -> list[dict]:
    """Fetch full bet history for a user — open and settled."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT b.*, p.market, p.confidence,
                   f.home_team, f.away_team, f.kickoff
            FROM bets b
            JOIN picks p ON b.pick_id = p.id
            JOIN fixtures f ON p.fixture_id = f.fixture_id
            WHERE b.user_id = ?
            ORDER BY b.placed_at DESC
        """, (user_id,)) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def settle_bet(bet_id: int, result: str, actual_return: float):
    """Settle a bet and update user totals."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE bets
            SET status = ?, actual_return = ?, settled_at = ?
            WHERE id = ?
        """, (result, actual_return, datetime.utcnow(), bet_id))

        # Update user running totals
        bet = await db.execute(
            "SELECT user_id, stake FROM bets WHERE id = ?", (bet_id,)
        )
        row = await bet.fetchone()
        if row:
            await db.execute("""
                UPDATE users SET
                    total_staked   = total_staked + ?,
                    total_returned = total_returned + ?
                WHERE user_id = ?
            """, (row[1], actual_return, row[0]))

        await db.commit()


async def get_bets_for_pick(pick_id: int) -> list[dict]:
    """Fetch all bets placed on a specific pick — used by result settler."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM bets WHERE pick_id = ? AND status = 'open'",
            (pick_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


# ─────────────────────────────────────────
# PERFORMANCE FUNCTIONS
# ─────────────────────────────────────────

async def get_performance_stats(user_id: int) -> dict:
    """Aggregate bet stats for /performance command."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Overall stats
        async with db.execute("""
            SELECT
                COUNT(*)                                      AS total_bets,
                SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN status='lost' THEN 1 ELSE 0 END) AS losses,
                SUM(stake)                                    AS total_staked,
                SUM(actual_return)                            AS total_returned,
                MAX(actual_return - stake)                    AS best_win,
                MIN(actual_return - stake)                    AS worst_loss
            FROM bets WHERE user_id = ? AND status != 'open'
        """, (user_id,)) as cursor:
            overall = dict(await cursor.fetchone())

        # Stats per market
        async with db.execute("""
            SELECT p.market,
                COUNT(*)                                       AS count,
                SUM(CASE WHEN b.status='won' THEN 1 ELSE 0 END) AS wins
            FROM bets b
            JOIN picks p ON b.pick_id = p.id
            WHERE b.user_id = ? AND b.status != 'open'
            GROUP BY p.market
            ORDER BY wins DESC
        """, (user_id,)) as cursor:
            by_market = [dict(r) for r in await cursor.fetchall()]

        # Stats per sport
        async with db.execute("""
            SELECT p.sport,
                COUNT(*)                                       AS count,
                SUM(CASE WHEN b.status='won' THEN 1 ELSE 0 END) AS wins,
                SUM(b.actual_return) - SUM(b.stake)            AS profit
            FROM bets b
            JOIN picks p ON b.pick_id = p.id
            WHERE b.user_id = ? AND b.status != 'open'
            GROUP BY p.sport
        """, (user_id,)) as cursor:
            by_sport = [dict(r) for r in await cursor.fetchall()]

        return {
            "overall":   overall,
            "by_market": by_market,
            "by_sport":  by_sport,
        }


# ─────────────────────────────────────────
# BACKTEST FUNCTIONS
# ─────────────────────────────────────────

async def get_settled_picks(sport: str, days: int) -> list[dict]:
    """Fetch settled picks for backtest — last N days."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT p.*, f.home_team, f.away_team, f.league
            FROM picks p
            JOIN fixtures f ON p.fixture_id = f.fixture_id
            WHERE p.sport = ?
              AND p.result != 'pending'
              AND p.created_at >= DATE('now', ? || ' days')
            ORDER BY p.created_at DESC
        """, (sport, f"-{days}")) as cursor:
            rows = await cursor.fetchall()
            result = []
            for r in rows:
                pick = dict(r)
                pick["qualifier_flags"] = json.loads(pick["qualifier_flags"])
                result.append(pick)
            return result