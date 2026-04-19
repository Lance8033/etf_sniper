import os
import datetime
from core.database import get_all_etfs
from parsers.factory import execute_history_sync

def main():
    print(f"[{datetime.datetime.now()}] Starting daily sync task...")
    
    # 1. Get all active ETFs
    try:
        df_config = get_all_etfs()
        if df_config.empty:
            print("No ETFs found in config.")
            return
            
        active_etfs = df_config[df_config['is_active'] == 1]
        print(f"Found {len(active_etfs)} active targets.")
        
        # 2. Execute sync
        for _, etf in active_etfs.iterrows():
            ticker = etf['ticker']
            name = etf['name']
            issuer = etf['issuer']
            parser_type = etf['parser_type']
            
            print(f"Syncing: {ticker} ({name})...")
            try:
                execute_history_sync(ticker, issuer, parser_type)
                print(f"OK: {ticker} sync complete.")
            except Exception as e:
                print(f"FAIL: {ticker} error: {e}")
                
        print(f"[{datetime.datetime.now()}] All tasks finished!")
        
    except Exception as e:
        print(f"系統錯誤: {e}")

if __name__ == "__main__":
    main()
