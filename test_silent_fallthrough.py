import sys
sys.path.append("/Users/NewUser/Desktop/pocketmoney-final")

def run_sim(chat_id, text, tier="whale_tier", is_admin=True, bs_exists=True):
    print(f"\n--- SIM: text='{text}', tier={tier}, admin={is_admin}, bs={bs_exists} ---")
    
    parts = text.strip().split()
    subcmd = parts[1].lower() if len(parts) > 1 else ""
    print(f"subcmd: '{subcmd}'")
    
    # /bonds — status
    if not subcmd or subcmd == "status":
        print("-> Entered status block")
        if tier == "free" and not is_admin:
            print("  -> Returned: free tier warning")
            return
        if not bs_exists:
            print("  -> Returned: init warning")
            return
        print("  -> Would send full status dashboard")
        return

    # Control commands: whale-only (or admin)
    print("-> Entered control block")
    if tier != "whale_tier" and not is_admin:
        print("  -> Returned: whale required warning")
        return

    if subcmd == "start":
        print("  -> Handled start")
    elif subcmd == "stop":
        print("  -> Handled stop")
    elif subcmd == "live":
        print("  -> Handled live")
    elif subcmd == "dryrun":
        print("  -> Handled dryrun")
    elif subcmd == "set" and len(parts) >= 4:
        print("  -> Handled set")
    elif subcmd == "history":
        print("  -> Handled history")
    else:
        print("  -> Fell through (unknown subcmd or 'set' with < 4 args), NO RETURN OR MESSAGE!")

run_sim("123", "/bonds")
run_sim("123", "/bonds ", is_admin=False, tier="pro")
run_sim("123", "/bonds unknown_command")
