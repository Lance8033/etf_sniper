import requests
import streamlit as st

@st.cache_data(ttl=3600)
def fetch_twse_etf_name(ticker: str) -> str:
    """從證交所開放資料抓取代碼對應的名稱"""
    try:
        url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            data = r.json()
            for stock in data:
                # API 欄位通常叫 Code 與 Name
                if stock.get('Code') == ticker:
                    return stock.get('Name')
    except Exception:
        pass
    return ""
