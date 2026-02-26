from config_loader import load_config
from telegram_bot import TelegramBotHandler
from execution_engine import ExecutionEngine
cfg = load_config("config.yaml")

bh = TelegramBotHandler(cfg)
ee = ExecutionEngine(cfg)
bh.execution_engine = ee

print(f"Direct attribute: {bh.execution_engine}")
try:
    print(f"Getattr: {getattr(bh, 'execution_engine', None)}")
    print(f"Bool evaluation: {bool(getattr(bh, 'execution_engine', None))}")
except Exception as e:
    print(f"Error evaluating eval: {e}")
