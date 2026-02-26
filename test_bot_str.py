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
        self.user_subs = {"123": {"tier": "whale_tier"}}  # SIMULATE WHALE TIER
        self.banned_users = set()

bot = MockBot(cfg)
bot._bond_spreader = BondSpreader(cfg)

def mock_send(chat_id, text, **kwargs):
    print("--- ATTEMPTING TO SEND ---")
    print(len(text), "chars")
    print(text)
    print("--------------------------")
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    try:
        json.dumps(payload)
        print("JSON Serialization: OK")
    except Exception as e:
        print("JSON Serialization: FAILED", e)

bot._send = mock_send

print("TESTING /bonds AS ADMIN/WHALE")
try:
    bot._cmd_bonds(bot.default_chat_id, "/bonds")
except Exception as e:
    import traceback
    traceback.print_exc()

print("\nTESTING /bonds AS RANDOM WHALE")
try:
    bot._cmd_bonds("123", "/bonds")
except Exception as e:
    import traceback
    traceback.print_exc()
