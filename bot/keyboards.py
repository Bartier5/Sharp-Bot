from telegram import InlineKeyboardButton, InlineKeyboardMarkup


# ─────────────────────────────────────────
# MAIN MENU
# ─────────────────────────────────────────

def main_menu_keyboard() -> InlineKeyboardMarkup:
    """
    Main menu shown on /start.
    Four quadrant layout covering all major features.
    """
    buttons = [
        [
            InlineKeyboardButton(
                "⚽ Today's Picks",  callback_data="today_football"
            ),
            InlineKeyboardButton(
                "🏀 NBA Picks",      callback_data="today_nba"
            ),
        ],
        [
            InlineKeyboardButton(
                "📋 My Bets",        callback_data="my_bets"
            ),
            InlineKeyboardButton(
                "📊 Performance",    callback_data="performance"
            ),
        ],
        [
            InlineKeyboardButton(
                "🔔 Briefing",       callback_data="briefing"
            ),
            InlineKeyboardButton(
                "⚙️ Settings",       callback_data="settings"
            ),
        ],
        [
            InlineKeyboardButton(
                "❓ Help",           callback_data="help"
            ),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


# ─────────────────────────────────────────
# SPORT PREFERENCE
# ─────────────────────────────────────────

def sport_preference_keyboard() -> InlineKeyboardMarkup:
    """Shown on /setpreference and first /start registration."""
    buttons = [
        [
            InlineKeyboardButton(
                "⚽ Football Only",  callback_data="pref_football"
            ),
        ],
        [
            InlineKeyboardButton(
                "🏀 NBA Only",       callback_data="pref_nba"
            ),
        ],
        [
            InlineKeyboardButton(
                "🌐 Both Sports",    callback_data="pref_both"
            ),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


# ─────────────────────────────────────────
# SETTINGS MENU
# ─────────────────────────────────────────

def settings_keyboard() -> InlineKeyboardMarkup:
    """Settings panel — preference and sample size."""
    buttons = [
        [
            InlineKeyboardButton(
                "🎯 Sport Preference",
                callback_data="settings_preference"
            ),
        ],
        [
            InlineKeyboardButton(
                "📏 Min Games (8–30)",
                callback_data="settings_mingames"
            ),
        ],
        [
            InlineKeyboardButton(
                "◀️ Back to Menu",
                callback_data="main_menu"
            ),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


# ─────────────────────────────────────────
# MIN GAMES SELECTOR
# ─────────────────────────────────────────

def min_games_keyboard() -> InlineKeyboardMarkup:
    """
    Quick-select buttons for minimum games threshold.
    Covers the full 8–30 range in meaningful steps.
    """
    buttons = [
        [
            InlineKeyboardButton("8",  callback_data="games_8"),
            InlineKeyboardButton("10", callback_data="games_10"),
            InlineKeyboardButton("12", callback_data="games_12"),
        ],
        [
            InlineKeyboardButton("15", callback_data="games_15"),
            InlineKeyboardButton("18", callback_data="games_18"),
            InlineKeyboardButton("20", callback_data="games_20"),
        ],
        [
            InlineKeyboardButton("25", callback_data="games_25"),
            InlineKeyboardButton("30", callback_data="games_30"),
        ],
        [
            InlineKeyboardButton(
                "◀️ Back",
                callback_data="settings"
            ),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


# ─────────────────────────────────────────
# PICKS LIST
# ─────────────────────────────────────────

def picks_keyboard(picks: list[dict]) -> InlineKeyboardMarkup:
    """
    Dynamic keyboard built from today's qualified picks.
    One button per pick — shows teams + confidence grade.
    """
    buttons = []
    for pick in picks:
        label = (
            f"[{pick['confidence']}] "
            f"{pick['home_team']} vs {pick['away_team']} "
            f"— {pick['market'].replace('_', ' ').upper()}"
        )
        buttons.append([
            InlineKeyboardButton(
                label,
                callback_data=f"pick_{pick['id']}"
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "◀️ Back to Menu",
            callback_data="main_menu"
        )
    ])
    return InlineKeyboardMarkup(buttons)


# ─────────────────────────────────────────
# SINGLE PICK ACTIONS
# ─────────────────────────────────────────

def pick_action_keyboard(pick_id: int) -> InlineKeyboardMarkup:
    """
    Actions available on a single pick detail view.
    Log Bet opens the bet flow. Odds refreshes live lines.
    """
    buttons = [
        [
            InlineKeyboardButton(
                "💰 Log Bet",
                callback_data=f"bet_{pick_id}"
            ),
            InlineKeyboardButton(
                "📈 Live Odds",
                callback_data=f"odds_{pick_id}"
            ),
        ],
        [
            InlineKeyboardButton(
                "◀️ Back to Picks",
                callback_data="today_all"
            ),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


# ─────────────────────────────────────────
# BET CONFIRMATION
# ─────────────────────────────────────────

def bet_confirm_keyboard(pick_id: int,
                          stake: float) -> InlineKeyboardMarkup:
    """
    Confirm or cancel a bet before it's logged.
    Shown after user provides stake amount.
    """
    buttons = [
        [
            InlineKeyboardButton(
                "✅ Confirm Bet",
                callback_data=f"confirm_bet_{pick_id}_{stake}"
            ),
            InlineKeyboardButton(
                "❌ Cancel",
                callback_data=f"cancel_bet_{pick_id}"
            ),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


# ─────────────────────────────────────────
# OPEN BETS
# ─────────────────────────────────────────

def open_bets_keyboard(bets: list[dict]) -> InlineKeyboardMarkup:
    """
    Dynamic keyboard for open bets.
    Each bet shows match + stake. Tapping opens bet detail.
    """
    buttons = []
    for bet in bets:
        label = (
            f"{bet['home_team']} vs {bet['away_team']} "
            f"— £{bet['stake']} @ {bet['odds']}"
        )
        buttons.append([
            InlineKeyboardButton(
                label,
                callback_data=f"bet_detail_{bet['id']}"
            )
        ])

    buttons.append([
        InlineKeyboardButton(
            "🔄 Settle Now",
            callback_data="settle"
        ),
        InlineKeyboardButton(
            "◀️ Back",
            callback_data="main_menu"
        ),
    ])
    return InlineKeyboardMarkup(buttons)


# ─────────────────────────────────────────
# BACKTEST
# ─────────────────────────────────────────

def backtest_keyboard() -> InlineKeyboardMarkup:
    """Sport and day range selector for /backtest."""
    buttons = [
        [
            InlineKeyboardButton(
                "⚽ Football",   callback_data="backtest_football"
            ),
            InlineKeyboardButton(
                "🏀 NBA",        callback_data="backtest_nba"
            ),
        ],
        [
            InlineKeyboardButton(
                "7 days",        callback_data="backtest_days_7"
            ),
            InlineKeyboardButton(
                "14 days",       callback_data="backtest_days_14"
            ),
            InlineKeyboardButton(
                "30 days",       callback_data="backtest_days_30"
            ),
        ],
        [
            InlineKeyboardButton(
                "◀️ Back",       callback_data="main_menu"
            ),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


# ─────────────────────────────────────────
# CONFIRM SETTLE
# ─────────────────────────────────────────

def settle_confirm_keyboard() -> InlineKeyboardMarkup:
    """Confirm manual settle trigger."""
    buttons = [
        [
            InlineKeyboardButton(
                "✅ Yes, settle now",
                callback_data="confirm_settle"
            ),
            InlineKeyboardButton(
                "❌ Cancel",
                callback_data="main_menu"
            ),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


# ─────────────────────────────────────────
# LEAGUES INFO
# ─────────────────────────────────────────

def leagues_keyboard() -> InlineKeyboardMarkup:
    """Mode selector for /leagues command."""
    buttons = [
        [
            InlineKeyboardButton(
                "🏟️ Original Mode",
                callback_data="leagues_original"
            ),
            InlineKeyboardButton(
                "⚡ HVLO Mode",
                callback_data="leagues_hvlo"
            ),
        ],
        [
            InlineKeyboardButton(
                "◀️ Back",
                callback_data="main_menu"
            ),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


# ─────────────────────────────────────────
# NAVIGATION HELPER
# ─────────────────────────────────────────

def back_to_menu() -> InlineKeyboardMarkup:
    """Single back button — reused across many views."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "◀️ Back to Menu",
            callback_data="main_menu"
        )
    ]])