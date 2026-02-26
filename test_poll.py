import yaml, requests
with open("/Users/NewUser/Desktop/pocketmoney-final/config.yaml", "r") as f:
    cfg = yaml.safe_load(f)
token = cfg.get("telegram", {}).get("bot_token")
if token:
    r = requests.get(f"https://api.telegram.org/bot{token}/getUpdates?limit=2")
    print(r.json())
else:
    print("No token")
