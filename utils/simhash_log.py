# utils/simhash_log.py
from datetime import datetime
import os

def save_simhash_removed(df, keyword):
    if df.empty:
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    log_dir = f"outputs/{keyword}/simhash_logs"
    os.makedirs(log_dir, exist_ok=True)

    path = os.path.join(log_dir, f"{timestamp}.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")
