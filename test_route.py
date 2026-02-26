import sys
sys.path.append("/Users/NewUser/Desktop/pocketmoney-final")
from telegram_bot import TelegramBotHandler

class FakeBot(TelegramBotHandler):
    def __init__(self):
        self.bot_token = "fake"
        self.default_chat_id = "123"
        self.banned_users = set()
    
    def _enrich_user(self, *args): pass
    def _cmd_bonds(self, *args): print("SUCCESS: _cmd_bonds CALLED")
    def _cmd_status(self, *args): print("SUCCESS: _cmd_status CALLED")
    def _cmd_start(self, *args): print("SUCCESS: _cmd_start CALLED")

bot = FakeBot()

print("\n--- Testing /status ---")
upd = {"message": {"chat": {"id": "123"}, "text": "/status"}}
bot._handle_update(upd)

print("\n--- Testing /bonds ---")
upd = {"message": {"chat": {"id": "123"}, "text": "/bonds"}}
bot._handle_update(upd)

print("\n--- Testing /bonds@botname ---")
upd = {"message": {"chat": {"id": "123"}, "text": "/bonds@PolyQuickbot"}}
bot._handle_update(upd)
