import sys
import yaml
import logging

logging.basicConfig(level=logging.DEBUG)

with open('/Users/NewUser/Desktop/pocketmoney-final/config.yaml', 'r') as f:
    cfg = yaml.safe_load(f)

print("Instantiating...")
sys.path.append('/Users/NewUser/Desktop/pocketmoney-final')
from execution_engine import ExecutionEngine
try:
    ee = ExecutionEngine(cfg)
    print("Success")
except Exception as e:
    import traceback
    traceback.print_exc()
