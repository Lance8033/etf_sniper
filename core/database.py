import streamlit as st
import pandas as pd
import datetime
import os
from supabase import create_client, Client

@st.cache_resource
def init_connection() -> Client:
    # 優先從環境變數讀取 (適用於 GitHub Actions / Docker)
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    
    # 如果環境變數沒抓到，再嘗試從 Streamlit Secrets 讀取 (適用於本地 UI)
    if not url or not key:
        try:
            url = st.secrets.get("SUPABASE_URL")
            key = st.secrets.get("SUPABASE_KEY")
        except Exception:
            pass
            
    if not url or not key:
        return None
    return create_client(url, key)

try:
    supabase = init_connection()
except Exception:
    supabase = None

def check_db_connection():
    if not supabase:
        st.error("🚨 無法連線至資料庫！請確認 `.streamlit/secrets.toml` 是否已填入正確的 `SUPABASE_URL` 與 `SUPABASE_KEY`。")
        st.stop()

def get_active_etfs() -> pd.DataFrame:
    """從 DB 取得所有啟用的 ETF 清單"""
    check_db_connection()
    response = supabase.table('etf_config').select('*').eq('is_active', 1).execute()
    return pd.DataFrame(response.data)

def get_all_etfs() -> pd.DataFrame:
    """從 DB 取得所有 ETF 清單 (不論啟用狀態)"""
    check_db_connection()
    response = supabase.table('etf_config').select('*').execute()
    return pd.DataFrame(response.data)

def add_etf_config(ticker: str, name: str, issuer: str, parser_type: str):
    """新增或更新一檔 ETF 的戰略配置"""
    check_db_connection()
    now = datetime.datetime.now().isoformat()
    data = {
        "ticker": ticker,
        "name": name,
        "issuer": issuer,
        "parser_type": parser_type,
        "is_active": 1,
        "last_updated": now
    }
    supabase.table('etf_config').upsert(data).execute()

def update_etf_config_status(df_updates: pd.DataFrame):
    """從 Data Editor 首頁整批更新 ETF 狀態"""
    check_db_connection()
    now = datetime.datetime.now().isoformat()
    for _, row in df_updates.iterrows():
        data = {
            "is_active": int(row['is_active']),
            "last_updated": now
        }
        supabase.table('etf_config').update(data).eq('ticker', row['ticker']).execute()

def delete_etf_config(ticker: str):
    """刪除一檔 ETF (並可選擇清除其歷史資料)"""
    check_db_connection()
    supabase.table('etf_config').delete().eq('ticker', ticker).execute()
    supabase.table('etf_holdings_history').delete().eq('ticker', ticker).execute()
