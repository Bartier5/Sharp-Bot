import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_FOOTBALL_KEY  = os.getenv("API_FOOTBALL_KEY")
ODDS_API_KEY      = os.getenv("ODDS_API_KEY")
#base urls
API_FOOTBALL_BASE = "https://v3.football.api-sports.io"
ODDS_API_BASE     = "https://api.the-odds-api.com/v4"
#db path
DB_PATH  = "db/sharp.db"
LOG_PATH = "logs/bot.log"

MODE_ORIGINAL = "original"
MODE_HVLO     = "hvlo"
MODE_NBA      = "nba"

ORIGINAL_LEAGUES = {
    "Scottish Premiership":   179,
    "Belgian Pro League":     144,
    "Eredivisie":             88,
    "Greek Super League":     197,
    "Hungarian Liga":         271,
    "Austrian Bundesliga":    218,
    "Swiss Super League":     207,
    "Danish Superliga":       119,
    "Norwegian Eliteserien":  103,
    "Portuguese Primeira":    94,
}

HVLO_LEAGUES = {
    "Norwegian Division 2":   107,
    "Norwegian Division 3":   108,
    "Swedish Division 1":     570,
    "Women's Super League":   253,
    "AFC Champions League":   17,
    "Asian Youth Cup":        706,
}

LEAGUE_MODE_MAP = {}
for name, lid in ORIGINAL_LEAGUES.items():
    LEAGUE_MODE_MAP[lid] = MODE_ORIGINAL
for name, lid in HVLO_LEAGUES.items():
    LEAGUE_MODE_MAP[lid] = MODE_HVLO
ORIG_MIN_GAMES          = 8          # minimum games played (default, user-configurable 8–30)
ORIG_MAX_GAMES          = 30         # user-configurable ceiling
ORIG_REST_DAYS_MIN      = 3          # both teams must have rested this many days
ORIG_REFEREE_CARD_LIMIT = 4.5        # avg yellows/game — above this = chaos ref
ORIG_CS_RATE            = 0.55       # clean sheet rate for Win to Nil
ORIG_OPP_GOALS_AWAY     = 1.0        # opponent goals/game away
ORIG_WIN_BY_2_RATE      = 0.55       # wins by 2+ vs bottom half (AH -1.5)
ORIG_LEAGUE_AVG_GOALS   = 2.8        # league avg goals/game (U1.5 0-20min)
ORIG_HOME_2H_SCORE_RATE = 0.70       # home scores in 2nd half
ORIG_AWAY_2H_CONCEDE    = 0.65       # away concedes in 2nd half
ORIG_HT_LEAD_RATE       = 0.60       # home leads at HT
ORIG_HT_WIN_FROM_LEAD   = 0.80       # home wins when leading at HT
ORIG_BTTS_HOME_SCORE    = 0.65       # home scores in X% of games (BTTS)
ORIG_BTTS_AWAY_SCORE    = 0.55       # away scores in X% of games (BTTS)
ORIG_NO_TEAM_3_LIMIT    = 1.5        # both teams avg goals/game (No Team 3+)
ORIG_HOME_AVG_GOALS     = 1.8        # home avg goals scored (AH -1.5)
ORIG_HOME_ML_PROB       = 0.80       # implied prob for home ML
ORIG_AWAY_ML_PROB       = 0.85       # implied prob for away ML

ORIG_FORM_HOME_WINS     = 3          # min wins in last 5 home games
ORIG_FORM_AWAY_LOSSES   = 3          # min losses in last 5 away games (away team)

ORIG_MAX_INJURIES       = 3          # max missing players before hard block
ORIG_KEY_POSITIONS      = {"Goalkeeper", "Attacker"}

# Line movement
ORIG_LINE_MOVE_THRESHOLD = 0.15      # >15% move against pick → Grade B

# Edge ceiling
ORIG_EDGE_CEILING        = 0.92      # implied prob above this = no value → Grade C
DERBY_FIXTURE_IDS: set = set()

HVLO_MIN_GAMES          = 8
HVLO_BTTS_HIT_RATE      = 0.65       # BTTS hits 65–70% in youth/reserve
HVLO_OVER_15_RATE       = 0.70       # Over 1.5 hits 70%+
HVLO_OVER_25_RATE       = 0.58       # Over 2.5 hits 58%+
HVLO_MIN_LEG_ODDS       = 1.10       # each acca leg minimum odds
HVLO_MAX_LEG_ODDS       = 1.40       # each acca leg maximum odds
HVLO_ACCA_MIN_LEGS      = 3          # minimum legs in acca
HVLO_ACCA_MAX_LEGS      = 5          # maximum legs in acca
HVLO_TARGET_ACCA_MIN    = 2.00       # target accumulator odds floor
HVLO_TARGET_ACCA_MAX    = 3.50       # target accumulator odds ceiling

# HVLO markets allowed
HVLO_MARKETS = [
    "over_1.5",
    "btts",
    "over_2.5",
    "home_win_or_draw",   # Home W/D
]

NBA_MIN_GAMES            = 8
NBA_HOME_ML_PROB         = 0.80
NBA_AWAY_ML_PROB         = 0.85
NBA_OPP_WIN_PCT          = 0.450
NBA_SPREAD_MARGIN_CAP    = 0.55      # spread ≤ 55% of avg winning margin
NBA_1H_ML_PROB           = 0.70
NBA_MAX_PARLAY_LEGS      = 3
NBA_LOSING_STREAK_MAX    = 2
NBA_FORM_HOME_WINS       = 3         # min wins in last 5 games
NBA_FORM_AVG_MARGIN      = 5.0       # avg winning margin min (points)
NBA_OFFENSE_RANK_CUTOFF  = 10        # top-10 offense for team total OVER

# NBA markets allowed
NBA_MARKETS = [
    "ml",
    "spread",
    "1h_ml",
    "team_total_over",
    "parlay_leg",
]

GRADE_A_MIN_ODDS = 1.50   # Grade A requires odds above this
GRADE_A = "A"
GRADE_B = "B"
GRADE_C = "C"             # near-miss — within 10% of any threshold
GRADE_VOID = "VOID"       # disqualifier fired or gate failed

NEAR_MISS_TOLERANCE = 0.10

MORNING_BRIEFING_HOUR    = 8     # 8:00 AM daily
ODDS_REFRESH_MINUTES     = 30    # every 30 minutes
RESULT_CHECKER_HOURS     = 2     # every 2 hours
PRICE_POLL_MINUTES       = 5     # fixture status check

MAX_COMPARE_COINS        = 5     # max fixtures in /compare
REQUEST_TIMEOUT          = 10    # aiohttp request timeout (seconds)
CACHE_TTL_SECONDS        = 300   # 5-minute cache for repeated fetches

