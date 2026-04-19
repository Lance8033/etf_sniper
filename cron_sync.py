import os
import datetime
from core.database import get_all_etfs
from parsers.factory import execute_history_sync

def main():
    print(f"[{datetime.datetime.now()}] 啟動每日自動同步任務...")
    
    # 1. 取得所有啟用的 ETF
    try:
        df_config = get_all_etfs()
        if df_config.empty:
            print("目前配置中沒有任何 ETF。")
            return
            
        active_etfs = df_config[df_config['is_active'] == 1]
        print(f"找到 {len(active_etfs)} 檔啟用中的觀察目標。")
        
        # 2. 依序執行同步
        for _, etf in active_etfs.iterrows():
            ticker = etf['ticker']
            name = etf['name']
            issuer = etf['issuer']
            parser_type = etf['parser_type']
            
            print(f"正在同步: {ticker} ({name})...")
            try:
                # 這裡調用我們在 factory.py 定義的同步邏輯
                # 它會自動清理舊資料並追蹤最新的數據 (包含回溯邏輯)
                execute_history_sync(ticker, issuer, parser_type)
                print(f"✅ {ticker} 同步完成。")
            except Exception as e:
                print(f"❌ {ticker} 同步失敗: {e}")
                
        print(f"[{datetime.datetime.now()}] 所有任務已完成！")
        
    except Exception as e:
        print(f"系統錯誤: {e}")

if __name__ == "__main__":
    main()
