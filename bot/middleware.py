import logging
from telegram import Update
from telegram.ext import BaseHandler, CallbackContext
from data.storage import upsert_user

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# USER REGISTRATION MIDDLEWARE
# ─────────────────────────────────────────

class UserMiddleware(BaseHandler):
    """
    Intercepts every incoming update before it reaches
    any command handler.

    Responsibilities:
    1. Upsert user into the users table on every message
    2. Log command activity with user ID and username
    3. Pass the update through to the correct handler

    Assigned priority 0 — runs before all command handlers
    which default to priority 1.
    """

    def __init__(self):
        # Priority -1 ensures this runs before all other handlers
        super().__init__(callback=self._handle)

    async def _handle(self,
                       update: Update,
                       context: CallbackContext) -> None:
        """
        Core middleware logic.
        Called on every update — message or callback query.
        """
        if not update:
            return

        # Extract user from message or callback query
        user = None
        if update.message:
            user = update.message.from_user
        elif update.callback_query:
            user = update.callback_query.from_user

        if not user:
            return

        # Upsert to users table
        try:
            await upsert_user(
                user_id=user.id,
                username=user.username or user.first_name,
            )
        except Exception as e:
            # Never let middleware crash the bot
            logger.error(f"Middleware upsert failed: {e}")

        # Log the activity
        if update.message and update.message.text:
            logger.info(
                f"User {user.id} (@{user.username}): "
                f"{update.message.text[:50]}"
            )
        elif update.callback_query:
            logger.info(
                f"User {user.id} (@{user.username}) "
                f"callback: {update.callback_query.data}"
            )

    def check_update(self, update: object) -> bool:
        """
        Tell python-telegram-bot whether this handler
        should process the given update.
        Returns True for all updates — middleware is universal.
        """
        return isinstance(update, Update)


# ─────────────────────────────────────────
# LOGGING SETUP
# Called once from main.py on startup
# ─────────────────────────────────────────

def setup_logging(log_path: str) -> None:
    """
    Configure structured file + console logging.
    Rotating file handler prevents log files growing unbounded.
    """
    import logging.handlers

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # ── File handler — rotating, max 5MB, keep 3 backups ─────────
    file_handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,   # 5MB
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)

    # ── Console handler ───────────────────────────────────────────
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # ── Formatter ─────────────────────────────────────────────────
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | "
            "%(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Suppress noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger(
        "telegram.ext"
    ).setLevel(logging.WARNING)

    logger.info("Logging initialized")