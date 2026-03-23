import json
import requests
import sys
from datetime import datetime

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
DISCORD_WEBHOOK_URL = open("discord.token").read().rstrip()
USERNAME = "Bitcoin Forwarder Bot"
AVATAR_URL = "https://bitcoin.org/img/icons/opengraph.png"

# ---------------------------------------------------------
# Helper: send Discord webhook
# ---------------------------------------------------------
def send_discord_message(message_type="general", message="Insert Test Here"):
    #def send_discord_message(txid: str, source_wallet: str = None, dest_address: str = None, amount: float = None, network: str = "mainnet", message="Sweep Detected"):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    '''
    content_lines = [
        f"💸 **{message} on {network}**",
        f"🕒 {timestamp}",
        f"🔹 **Transaction ID:** `{txid}`"
    ]
    if source_wallet:
        content_lines.append(f"🔹 **Source wallet:** {source_wallet}")
    if dest_address:
        content_lines.append(f"🔹 **Destination:** `{dest_address}`")
    if amount != None:
        content_lines.append(f"🔹 **Amount:** ${amount:.2f}")

    content = "\n".join(content_lines)
    '''
    content = message
    payload = {
        "username": USERNAME,
        "avatar_url": AVATAR_URL,
        "embeds": [{
            "description": content,
            "color": 0xF7931A  # Bitcoin orange
        }]
    }

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        if response.status_code not in (200, 204):
            print(f"[!] Discord webhook failed ({response.status_code}): {response.text}")
        else:
            print(f"[+] Sent Discord Message")
    except Exception as e:
        print(f"[!] Discord webhook error: {e}")


# ---------------------------------------------------------
# Command-line use
# ---------------------------------------------------------
if __name__ == "__main__":
    # Example usage:
    # python discord_notify.py <txid> [source_wallet] [dest_address] [amount_btc] [network]
    txid = sys.argv[1] if len(sys.argv) > 1 else "unknown"
    source_wallet = sys.argv[2] if len(sys.argv) > 2 else None
    dest_address = sys.argv[3] if len(sys.argv) > 3 else None
    amount = float(sys.argv[4]) if len(sys.argv) > 4 else None
    network = sys.argv[5] if len(sys.argv) > 5 else "mainnet"

    send_discord_message("genera", "testing")

