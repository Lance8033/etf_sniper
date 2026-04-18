import os
import datetime
import random
from typing import List, Dict

import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client, Client

# ==========================================
# 0. Page Config & Initialization
# ==========================================
st.set_page_config(page_title="主動 ETF 籌碼狙擊鏡", layout="wide", page_icon="🎯")

# ==========================================
# 1. Database Management (Supabase)
# ==========================================
@st.cache_resource
def init_connection() -> Client:
    url = st.secrets.get("SUPABASE_URL")
    key = st.secrets.get("SUPABASE_KEY")
    if not url or not key:
        return None
    return create_client(url, key)

try:
    supabase = init_connection()
except Exception as e:
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

def add_etf_config(ticker, name, issuer, parser_type):
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

def delete_etf_config(ticker):
    """刪除一檔 ETF (並可選擇清除其歷史資料)"""
    check_db_connection()
    supabase.table('etf_config').delete().eq('ticker', ticker).execute()
    supabase.table('etf_holdings_history').delete().eq('ticker', ticker).execute()

# ==========================================
# 2. Dynamic Parser Factory (動態解析器工廠模式)
# ==========================================
class BaseParser:
    """解析器基底介面"""
    def __init__(self, ticker, issuer):
        self.ticker = ticker
        self.issuer = issuer
        
    def validate(self) -> bool:
        """實作：測試連線與解析能力，供新增配置時阻擋無效 Parser"""
        raise NotImplementedError

    def fetch_data(self, date) -> pd.DataFrame:
        """實作：抓取指定日期的籌碼"""
        raise NotImplementedError

class HtmlTableParser(BaseParser):
    def validate(self):
        return True # 假裝置驗證成功
        
    def fetch_data(self, date) -> pd.DataFrame:
        # Mocking data specifically for demonstration
        return self._generate_mock_data(date)
        
    def _generate_mock_data(self, date):
        stocks = [("2330", "台積電"), ("2317", "鴻海"), ("2454", "聯發科"), ("2308", "台達電"), ("3008", "大立光")]
        data = []
        for sym, name in stocks:
            # 加上一點隨機雜訊，模擬每日權重浮動
            random_weight = round(random.uniform(2.0, 15.0), 2)
            random_shares = int(random_weight * 1000)
            data.append({
                "ticker": self.ticker,
                "date": date,
                "stock_symbol": sym,
                "stock_name": name,
                "shares": random_shares,
                "weight": random_weight
            })
        return pd.DataFrame(data)

class CsvDownloadParser(BaseParser):
    def validate(self):
        return True
        
    def fetch_data(self, date) -> pd.DataFrame:
        return HtmlTableParser(self.ticker, self.issuer).fetch_data(date) # 使用一樣的 mock

# Parser Factory Mapping
PARSER_REGISTRY = {
    "網頁表格型 (HTML Table)": HtmlTableParser,
    "CSV下載型 (CSV Download)": CsvDownloadParser
}

def get_parser(parser_type, ticker, issuer):
    """獲取解析器實例"""
    parser_class = PARSER_REGISTRY.get(parser_type)
    if not parser_class:
        raise ValueError(f"Unknown parser type: {parser_type}")
    return parser_class(ticker, issuer)

def populate_mock_history_for_ticker(ticker, issuer, parser_type):
    """此為展示用：當新增 ETF 時，自動產生前五個交易日的歷史資料"""
    check_db_connection()
    parser = get_parser(parser_type, ticker, issuer)
    today = datetime.date.today()
    dates = [(today - datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(5, -1, -1)]
    
    for date in dates:
        df = parser.fetch_data(date)
        records = df.to_dict(orient='records')
        if records:
            # upsert ignores duplicates by default if the unique constraint is met, 
            # but since we haven't set up the advanced onConflict yet, we just execute.
            try:
                supabase.table('etf_holdings_history').upsert(records).execute()
            except Exception as e:
                pass # ignore uniqueness failures if re-running

# ==========================================
# 3. Streamlit User Interface
# ==========================================

st.title("🎯 主動 ETF 籌碼狙擊鏡 - 動態偵察版")
st.markdown("千萬資產免稅複利引擎核心系統 (Cloud Native Edition ☁️)")

# 優先檢查連線配置
if not supabase:
    st.error("🚨 系統偵測到 Supabase 連線尚未設定或失敗。請參考文件設定 `secrets.toml`。")
    st.info("架構已完全升級為 GitHub + Supabase。這代表您不再需要本地資料庫囉！")
    st.stop()

# Navigation
tabs = st.tabs(["⚙️ 戰略配置中心", "⚔️ 每日突擊訊號 (Diff)", "📈 時空夜視鏡 (Trend Chart)"])

# ----------------- Tab 1: 戰略配置中心 -----------------
with tabs[0]:
    st.header("戰略配置中心")
    st.markdown("在這裡管理並調整您的觀察火力，拒絕程式硬編碼。資料即時寫入 **Supabase**，永不遺失。")
    
    # 建立一個 Form 來輸入新 ETF
    with st.expander("➕ 新增觀察對象 (Add New ETF Target)", expanded=True):
        with st.form("add_etf_form", clear_on_submit=True):
            col1, col2 = st.columns(2)
            new_ticker = col1.text_input("ETF 代號 (Ticker)")
            new_name = col2.text_input("ETF 名稱 (Name)")
            
            col3, col4 = st.columns(2)
            new_issuer = col3.selectbox("投信發行商 (Issuer)", ["元大", "國泰", "群益", "富邦", "復華", "中信"])
            new_parser = col4.selectbox("解析器類型 (Parser Type)", list(PARSER_REGISTRY.keys()))
            
            submit = st.form_submit_button("新增入列 🚀")
            
            if submit:
                if new_ticker and new_name:
                    try:
                        # 測試驗證
                        parser_instance = get_parser(new_parser, new_ticker, new_issuer)
                        if parser_instance.validate():
                            add_etf_config(new_ticker, new_name, new_issuer, new_parser)
                            # 因為是新加入，自動產生一些假歷史資料好做圖表展示
                            populate_mock_history_for_ticker(new_ticker, new_issuer, new_parser)
                            st.success(f"成功: {new_ticker} {new_name} 已加入雲端偵察序列！")
                        else:
                            st.error(f"解析器驗證失敗，無法偵測 {new_issuer} 的配置。")
                    except Exception as e:
                        st.error(f"系統錯誤: {e}")
                else:
                    st.warning("請填寫完整的代號與名稱！")

    # 列出現有 DB 中的資料並用 Data_Editor 直接編輯狀態
    st.subheader("📋 現有觀測序列管理")
    df_config = get_all_etfs()
    
    if not df_config.empty:
        # 轉換 boolean 以便在 Editor 中使用 Checkbox
        df_config['is_active'] = df_config['is_active'].astype(bool)
        
        edited_df = st.data_editor(
            df_config,
            column_config={
                "is_active": st.column_config.CheckboxColumn("是否啟用偵察?", default=True),
                "ticker": st.column_config.TextColumn("代號", disabled=True),
                "name": st.column_config.TextColumn("名稱", disabled=True),
                "issuer": st.column_config.TextColumn("投信", disabled=True),
                "parser_type": st.column_config.TextColumn("解析器", disabled=True),
                "last_updated": st.column_config.DatetimeColumn("最後更新時間", disabled=True)
            },
            hide_index=True,
            use_container_width=True,
            key='config_editor'
        )
        
        col_s, col_d = st.columns([1, 1])
        # 按鈕：儲存變更
        if col_s.button("💾 儲存狀態變更"):
            edited_df['is_active'] = edited_df['is_active'].astype(int)
            update_etf_config_status(edited_df)
            st.success("配置狀態已成功同步至 Supabase 雲端資料庫。")
            st.rerun()
            
        # 按鈕：刪除選定的 ETF
        ticker_to_delete = col_d.selectbox("選擇要刪除撤防的 ETF", df_config['ticker'].tolist())
        if col_d.button("🗑️ 刪除並撤除資料"):
            delete_etf_config(ticker_to_delete)
            st.success(f"{ticker_to_delete} 及其雲端歷史數據已全面清除。")
            st.rerun()
    else:
        st.info("目前 Supabase 資料庫中沒有任何觀察目標，請從上方表單新增。")

# ----------------- Tab 2: 每日突擊訊號 (Diff) -----------------
with tabs[1]:
    st.header("⚔️ 影子比對引擎 - 每日持股突擊訊號")
    
    active_etfs = get_active_etfs()
    if active_etfs.empty:
        st.warning("目前沒有啟用中的 ETF，請回「戰略配置中心」設定。")
    else:
        target_etf = st.sidebar.selectbox("🎯 選擇偵察目標", active_etfs['ticker'] + " - " + active_etfs['name'])
        target_ticker = target_etf.split(" - ")[0]
        
        st.markdown(f"**目前鎖定目標**: {target_etf}")
        
        # 從 Supabase 拿這個 ETF 的最近資料
        response = supabase.table('etf_holdings_history').select('*').eq('ticker', target_ticker).order('date', desc=True).execute()
        df_history = pd.DataFrame(response.data)

        if not df_history.empty:
            available_dates = df_history['date'].unique()
            if len(available_dates) >= 2:
                # 取最新的兩天來做 Diff
                date_new = available_dates[0]
                date_old = available_dates[1]
                st.write(f"正在與 Supabase 雲端比對 **{date_new}** 與 前一交易日 **{date_old}** 的變化...")
                
                df_new = df_history[df_history['date'] == date_new][['stock_symbol', 'stock_name', 'weight', 'shares']]
                df_old = df_history[df_history['date'] == date_old][['stock_symbol', 'stock_name', 'weight', 'shares']]
                
                df_new = df_new.rename(columns={'weight': '權重(新)', 'shares': '股數(新)'})
                df_old = df_old.rename(columns={'weight': '權重(舊)', 'shares': '股數(舊)'})
                
                # 核心 Diff 運算
                df_diff = pd.merge(df_old, df_new, on=['stock_symbol', 'stock_name'], how='outer')
                df_diff.fillna(0, inplace=True)
                df_diff['權重差異 (%)'] = df_diff['權重(新)'] - df_diff['權重(舊)']
                
                def highlight_diff(row):
                    if row['權重(舊)'] == 0 and row['權重(新)'] > 0:
                        return ['background-color: #004d00; color: #ccffcc'] * len(row) # 新兵 (Dark Green)
                    elif row['權重(新)'] == 0 and row['權重(舊)'] > 0:
                        return ['background-color: #660000; color: #ffcccc'] * len(row) # 徹底剔除 (Dark Red)
                    elif row['權重差異 (%)'] >= 1.0:
                        return ['background-color: #1a3300'] * len(row) # 加碼老兵
                    elif row['權重差異 (%)'] <= -1.0:
                        return ['background-color: #330000'] * len(row) # 減碼老兵
                    return [''] * len(row)
                
                styled_df = df_diff.style.apply(highlight_diff, axis=1).format({
                    '權重(新)': "{:.2f}%", '權重(舊)': "{:.2f}%", '權重差異 (%)': "{:+.2f}%",
                    '股數(新)': "{:,.0f}", '股數(舊)': "{:,.0f}"
                })
                
                st.dataframe(styled_df, use_container_width=True)
                st.markdown("> **說明**: 深綠底為新進成份股(新兵)，深紅底為完全剔除；微綠/微紅底代表權重增減超過 1%。")
            else:
                st.info(f"目標 {target_ticker} 在雲端的歷史資料不足兩天，無法進行 Diff 比較。")
        else:
             st.info(f"並未在 Supabase 搜尋到 {target_ticker} 的任何歷史資料。")

# ----------------- Tab 3: 多天時空夜視鏡 (Trend Chart) -----------------
with tabs[2]:
    st.header("📈 時空夜視鏡 - 籌碼趨勢雷達")
    
    if active_etfs.empty:
        st.warning("目前沒有啟用中的 ETF，請回「戰略配置中心」設定。")
    else:
        trend_ticker = st.selectbox("🎯 選擇要觀看趨勢的 ETF", active_etfs['ticker'], key="trend_ticker")
        
        response = supabase.table('etf_holdings_history').select('date, stock_symbol, stock_name, weight').eq('ticker', trend_ticker).order('date').execute()
        df_trend = pd.DataFrame(response.data)
        
        if not df_trend.empty:
            symbols_available = df_trend['stock_symbol'].unique()
            selected_stock = st.multiselect("選擇欲觀察的成份股 (可複選)", symbols_available, default=symbols_available[:2])
            
            if selected_stock:
                plot_data = df_trend[df_trend['stock_symbol'].isin(selected_stock)]
                plot_data['label'] = plot_data['stock_symbol'] + " " + plot_data['stock_name']
                
                fig = px.line(plot_data, x='date', y='weight', color='label', markers=True,
                              title=f'{trend_ticker} 重點成分股權重變化趨勢 (Cloud Data)',
                              labels={'date': '日期', 'weight': '權重 (%)', 'label': '個股'})
                
                fig.update_layout(hovermode="x unified", template='plotly_dark')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("請於上方選單選擇至少一檔成份股。")
        else:
            st.info(f"目前雲端無 {trend_ticker} 歷史趨勢數據。")
