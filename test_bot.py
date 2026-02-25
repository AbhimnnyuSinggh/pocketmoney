import sys, json, time
sys.path.append("/Users/NewUser/Desktop/pocketmoney-final")
import config_loader
from telegram_bot import TelegramBotHandler
from bond_spreader import BondSpreader

cfg = config_loader.load_config("/Users/NewUser/Desktop/pocketmoney-final/config.yaml")

class MockBot(TelegramBotHandler):
    def __init__(self, cfg):
        self.bot_token = cfg.get("telegram", {}).get("bot_token", "")
        self.default_chat_id = str(cfg.get("telegram", {}).get("chat_id", ""))
        self.user_prefs = {}
        self.user_subs = {}
        self.banned_users = set()

bot = MockBot(cfg)
bot._bond_spreader = BondSpreader(cfg)

def mock_send(chat_id, text, **kwargs):
    print(f"BOT SENT: {text}")

bot._send = mock_send
bot._is_admin = lambda cid: True

print("=== Testing /bonds ===")
try:
    bot._cmd_bonds("123", "/bonds")
except Exception as e:
    import traceback
    traceback.print_exc()

print("=== Testing /bonds dryrun ===")
try:
    bot._cmd_bonds("123", "/bonds dryrun")
except Exception as e:
    import traceback
    traceback.print_exc()
