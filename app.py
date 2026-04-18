import os
import datetime
import random
from typing import List, Dict

import streamlit as st
import pandas as pd
import plotly.express as px
import requests
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
        # 利用日期的單雙數來動態模擬「新增建倉」與「全數清倉」
        day = int(date.split('-')[-1])
        if day % 2 == 0:
            stocks.append(("2382", "廣達")) # 偶數日有廣達
        else:
            stocks.append(("2303", "聯電")) # 奇數日有聯電
            
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
# 2.5 TWSE Open Data (證交所 API 自動查找)
# ==========================================
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
    except Exception as e:
        pass
    return ""

ISSUER_MAPPING = {
    "元大": ("元大", "網頁表格型 (HTML Table)"),
    "國泰": ("國泰", "CSV下載型 (CSV Download)"),
    "富邦": ("富邦", "網頁表格型 (HTML Table)"),
    "群益": ("群益", "網頁表格型 (HTML Table)"),
    "復華": ("復華", "CSV下載型 (CSV Download)"),
    "中信": ("中信", "網頁表格型 (HTML Table)"),
    "統一": ("統一", "網頁表格型 (HTML Table)"),
    "凱基": ("凱基", "網頁表格型 (HTML Table)"),
    "台新": ("台新", "網頁表格型 (HTML Table)"),
    "兆豐": ("兆豐", "網頁表格型 (HTML Table)"),
    "永豐": ("永豐", "網頁表格型 (HTML Table)"),
    "野村": ("野村", "網頁表格型 (HTML Table)"),
    "大華": ("大華銀", "網頁表格型 (HTML Table)"),
    "第一金": ("第一金", "網頁表格型 (HTML Table)"),
    "新光": ("新光", "網頁表格型 (HTML Table)"),
    "街口": ("街口", "網頁表格型 (HTML Table)"),
}

ALL_ISSUERS = [
    "元大", "國泰", "群益", "富邦", "復華", "中信", 
    "統一", "凱基", "台新", "兆豐", "永豐", "野村", 
    "大華銀", "第一金", "新光", "街口"
]

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

# Navigation Sidebar
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/3256/3256903.png", width=50) # 一個小icon
st.sidebar.markdown("### 🎯 狙擊偵察目標")
st.sidebar.markdown("---")
active_etfs = get_active_etfs()
if not active_etfs.empty:
    options = (active_etfs['ticker'] + " - " + active_etfs['name']).tolist()
    
    # 初始化選項狀態
    if 'selected_target_etf' not in st.session_state or st.session_state['selected_target_etf'] not in options:
        st.session_state['selected_target_etf'] = options[0]
        
    for opt in options:
        # 被選中的目標顯示醒目按鈕(primary)，其餘為一般按鈕(secondary)
        btn_type = "primary" if st.session_state['selected_target_etf'] == opt else "secondary"
        if st.sidebar.button(opt, use_container_width=True, type=btn_type):
            st.session_state['selected_target_etf'] = opt
            st.rerun()
            
    target_etf = st.session_state['selected_target_etf']
    global_target_ticker = target_etf.split(" - ")[0]
else:
    global_target_ticker = None
    target_etf = None

# Main Content Navigation
tabs = st.tabs(["📋 最新持股 (Current)", "⚔️ 每日突擊訊號 (Diff)", "📈 時空夜視鏡 (Trend Chart)", "📡 投信共識雷達", "⚙️ 戰略配置中心"])

# ----------------- Tab 5: 戰略配置中心 -----------------
with tabs[4]:
    st.header("戰略配置中心")
    
    if 'is_admin' not in st.session_state:
        st.session_state['is_admin'] = False

    # 要求密碼登入
    if not st.session_state['is_admin']:
        st.warning("🔒 核心機房區域已上鎖。目前為**訪客模式**觀看資料。只有管理員才能新增或撤防追蹤任務。")
        pwd = st.text_input("請輸入最高指揮官 (Admin) 密碼：", type="password")
        if st.button("解鎖權限"):
            # 從 secrets 抓密碼，若未設定則預設為 "admin123"
            correct_pwd = st.secrets.get("ADMIN_PASSWORD", "admin123")
            if pwd == correct_pwd:
                st.session_state['is_admin'] = True
                st.rerun()
            else:
                st.error("❌ 存取拒絕，密碼錯誤！")
                
    # 成功解鎖後才顯示以下管理內容
    if st.session_state['is_admin']:
        col1, col2 = st.columns([8, 2])
        col1.success("✅ **最高指揮官身分驗證完成**，武器系統已解鎖。資料將即時寫入 **Supabase** 雲端。")
        if col2.button("登出 (撤銷權限)"):
            st.session_state['is_admin'] = False
            st.rerun()
            
        # 建立一個 Form 來輸入新 ETF
        with st.expander("➕ 新增觀察對象 (Add New ETF Target)", expanded=True):
            st.markdown("💡 **第一步：主動偵測** (輸入代號後按 Enter，系統會連線證交所並推論出其餘資訊)")
            auto_ticker = st.text_input("請輸入 ETF 代號 (Ticker) 例: 0050", key="input_ticker")
            
            auto_name = ""
            auto_issuer = "元大" # 預設值
            auto_parser = "網頁表格型 (HTML Table)" # 預設值
            
            if auto_ticker:
                with st.spinner("🚀 正在向證券交易所連線查詢名稱與發行商..."):
                    fetched_name = fetch_twse_etf_name(auto_ticker)
                    if fetched_name:
                        auto_name = fetched_name
                        # 簡單推論發行商
                        for keyword, (issuer, parser) in ISSUER_MAPPING.items():
                            if keyword in auto_name:
                                auto_issuer = issuer
                                auto_parser = parser
                                break
                        st.success(f"✅ 自動查獲目標：**{auto_name}** | 系統推測投信：**{auto_issuer}**")
                    else:
                        st.warning("⚠️ 證交所公開資料中並未尋獲此代號，請在下方手動填寫補充。")
            
            st.markdown("---")
            with st.form("add_etf_form", clear_on_submit=True):
                st.markdown("💡 **第二步：確認與發射** (檢查下方自動帶入的參數，確認無誤後即可送出)")
                col1, col2 = st.columns(2)
                # 使用 value= 將抓取到的資料填入
                new_ticker = col1.text_input("ETF 代號 (Ticker)", value=auto_ticker)
                new_name = col2.text_input("ETF 名稱 (Name)", value=auto_name)
                
                col3, col4 = st.columns(2)
                issuer_idx = ALL_ISSUERS.index(auto_issuer) if auto_issuer in ALL_ISSUERS else 0
                new_issuer = col3.selectbox("投信發行商 (Issuer)", ALL_ISSUERS, index=issuer_idx)
                
                parser_keys = list(PARSER_REGISTRY.keys())
                parser_idx = parser_keys.index(auto_parser) if auto_parser in parser_keys else 0
                new_parser = col4.selectbox("解析器類型 (Parser Type)", parser_keys, index=parser_idx)
                
                submit = st.form_submit_button("確認新增入列 🚀")
                
                if submit:
                    if new_ticker and new_name:
                        try:
                            # 測試驗證
                            parser_instance = get_parser(new_parser, new_ticker, new_issuer)
                            if parser_instance.validate():
                                add_etf_config(new_ticker, new_name, new_issuer, new_parser)
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
            if col_s.button("💾 儲存狀態變更"):
                edited_df['is_active'] = edited_df['is_active'].astype(int)
                update_etf_config_status(edited_df)
                st.success("配置狀態已成功同步至 Supabase 雲端資料庫。")
                st.rerun()
                
            ticker_to_delete = col_d.selectbox("選擇要刪除撤防的 ETF", df_config['ticker'].tolist())
            if col_d.button("🗑️ 刪除並撤除資料"):
                delete_etf_config(ticker_to_delete)
                st.success(f"{ticker_to_delete} 及其雲端歷史數據已全面清除。")
                st.rerun()
        else:
            st.info("目前 Supabase 資料庫中沒有任何觀察目標，請從上方表單新增。")

# ----------------- Tab 1: 最新持股 (Current) -----------------
with tabs[0]:
    st.header("📋 最新持股火力分佈")
    if not global_target_ticker:
        st.warning("👈 請先從左側選單選擇偵察目標，或回到「戰略配置中心」設定啟用。")
    else:
        st.markdown(f"**目前鎖定目標**: {target_etf}")
        response = supabase.table('etf_holdings_history').select('*').eq('ticker', global_target_ticker).order('date', desc=True).execute()
        df_history = pd.DataFrame(response.data)
        
        if not df_history.empty:
            latest_date = df_history['date'].iloc[0]
            st.success(f"📊 最新資料日期：**{latest_date}**")
            df_latest = df_history[df_history['date'] == latest_date][['stock_symbol', 'stock_name', 'shares', 'weight']]
            df_latest = df_latest.rename(columns={'stock_symbol': '股票代號', 'stock_name': '股票名稱', 'shares': '持有股數', 'weight': '權重比例 (%)'})
            # 排序依據權重
            df_latest = df_latest.sort_values(by='權重比例 (%)', ascending=False).reset_index(drop=True)
            
            # 使用 Pandas Styler 背景漸層 (紅色系)，並保有千分位與百分比格式
            styled_curr = df_latest.style.format({
                '持有股數': "{:,.0f}",
                '權重比例 (%)': "{:.2f}%"
            }).background_gradient(subset=['權重比例 (%)'], cmap='Reds')
            
            st.dataframe(
                styled_curr, 
                use_container_width=True, 
                hide_index=True,
                height=(len(df_latest) + 1) * 35 + 3
            )
        else:
            st.info(f"並未在 Supabase 搜尋到 {global_target_ticker} 的歷史資料。")

# ----------------- Tab 2: 每日突擊訊號 (Diff) -----------------
with tabs[1]:
    st.header("⚔️ 影子比對引擎 - 每日持股突擊訊號")
    
    if not global_target_ticker:
        st.warning("👈 請先從左側選單選擇偵察目標。")
    else:
        st.markdown(f"**目前鎖定目標**: {target_etf}")
        
        # 從 Supabase 拿這個 ETF 的最近資料
        response = supabase.table('etf_holdings_history').select('*').eq('ticker', global_target_ticker).order('date', desc=True).execute()
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
                    # 使用較明亮且清爽的色系 (淺綠 / 淺紅)，並強制文字為黑色以確保絕佳可讀性
                    if row['權重(舊)'] == 0 and row['權重(新)'] > 0:
                        return ['background-color: #a8f0c6; color: #000000'] * len(row) # 新兵入列 (明顯淺綠)
                    elif row['權重(新)'] == 0 and row['權重(舊)'] > 0:
                        return ['background-color: #ffb3b3; color: #000000'] * len(row) # 徹底剔除 (明顯淺紅)
                    elif row['權重差異 (%)'] >= 1.0:
                        return ['background-color: #e6ffed; color: #000000'] * len(row) # 加碼老兵 (微綠)
                    elif row['權重差異 (%)'] <= -1.0:
                        return ['background-color: #ffe6e6; color: #000000'] * len(row) # 減碼老兵 (微紅)
                    return [''] * len(row)
                
                styled_df = df_diff.style.apply(highlight_diff, axis=1).format({
                    '權重(新)': "{:.2f}%", '權重(舊)': "{:.2f}%", '權重差異 (%)': "{:+.2f}%",
                    '股數(新)': "{:,.0f}", '股數(舊)': "{:,.0f}"
                })
                
                st.dataframe(styled_df, use_container_width=True)
                st.markdown("> **說明**: 淺綠底為新進成份股(新兵)，淺紅底為完全剔除；微綠/微紅底代表權重增減超過 1%。")
            else:
                st.info(f"目標 {global_target_ticker} 在雲端的歷史資料不足兩天，無法進行 Diff 比較。")
        else:
             st.info(f"並未在 Supabase 搜尋到 {global_target_ticker} 的任何歷史資料。")

# ----------------- Tab 3: 時空夜視鏡 (Trend Chart) -----------------
with tabs[2]:
    st.header("📈 時空夜視鏡 - 籌碼趨勢雷達")
    
    if not global_target_ticker:
        st.warning("👈 請先從左側選單選擇偵察目標。")
    else:
        st.markdown(f"**目前鎖定目標**: {target_etf}")
        response = supabase.table('etf_holdings_history').select('date, stock_symbol, stock_name, weight').eq('ticker', global_target_ticker).order('date').execute()
        df_trend = pd.DataFrame(response.data)
        
        if not df_trend.empty:
            symbols_available = df_trend['stock_symbol'].unique()
            selected_stock = st.multiselect("選擇欲觀察的成份股 (可複選)", symbols_available, default=symbols_available[:2])
            
            if selected_stock:
                plot_data = df_trend[df_trend['stock_symbol'].isin(selected_stock)]
                plot_data['label'] = plot_data['stock_symbol'] + " " + plot_data['stock_name']
                
                fig = px.line(plot_data, x='date', y='weight', color='label', markers=True,
                              title=f'{global_target_ticker} 重點成分股權重變化趨勢 (Cloud Data)',
                              labels={'date': '日期', 'weight': '權重 (%)', 'label': '個股'})
                
                fig.update_layout(hovermode="x unified", template='plotly_dark')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("請於上方選單選擇至少一檔成份股。")
        else:
            st.info(f"目前雲端無 {global_target_ticker} 歷史趨勢數據。")

# ----------------- Tab 4: 投信共識雷達 (Consensus Radar) -----------------
with tabs[3]:
    st.header("📡 投信共識雷達 - 跨 ETF 聯合建倉追蹤")
    st.markdown("宏觀掃描全市場，找出多檔 ETF **正在不約而同聯手買進** 的共識潛力股。")
    
    active_etfs_for_radar = get_active_etfs()
    
    if active_etfs_for_radar.empty:
        st.warning("目前沒有啟用中的 ETF，請回「戰略配置中心」設定。")
    else:
        active_tickers = active_etfs_for_radar['ticker'].tolist()
        
        # 抓取這些 ETF 所有的持股歷史
        response = supabase.table('etf_holdings_history').select('*').in_('ticker', active_tickers).order('date', desc=True).execute()
        df_all = pd.DataFrame(response.data)
        
        if not df_all.empty:
            consensus_positive = []
            consensus_negative = []
            
            # 對每一個 ETF 分別計算最新的 Diff
            for ticker in active_tickers:
                df_ticker = df_all[df_all['ticker'] == ticker]
                dates = df_ticker['date'].unique()
                if len(dates) >= 2:
                    date_new = dates[0]
                    date_old = dates[1]
                    
                    df_new = df_ticker[df_ticker['date'] == date_new][['stock_symbol', 'stock_name', 'weight']]
                    df_old = df_ticker[df_ticker['date'] == date_old][['stock_symbol', 'stock_name', 'weight']]
                    
                    df_diff = pd.merge(df_old, df_new, on=['stock_symbol', 'stock_name'], how='outer', suffixes=('_old', '_new'))
                    df_diff.fillna(0, inplace=True)
                    df_diff['diff_weight'] = df_diff['weight_new'] - df_diff['weight_old']
                    
                    # 過濾正向加碼與負向減碼
                    df_diff_pos = df_diff[df_diff['diff_weight'] > 0.001]
                    df_diff_neg = df_diff[df_diff['diff_weight'] < -0.001]
                    
                    for _, row in df_diff_pos.iterrows():
                        consensus_positive.append({
                            'stock_symbol': row['stock_symbol'], 'stock_name': row['stock_name'],
                            'source_etf': ticker, 'diff_weight': row['diff_weight']
                        })
                    for _, row in df_diff_neg.iterrows():
                        consensus_negative.append({
                            'stock_symbol': row['stock_symbol'], 'stock_name': row['stock_name'],
                            'source_etf': ticker, 'diff_weight': abs(row['diff_weight']) # 取絕對值方便後續排序與觀看
                        })
            
            st.subheader("🔥 投信聯手加碼 (建倉) 共識")
            if consensus_positive:
                df_pos = pd.DataFrame(consensus_positive)
                df_grouped_pos = df_pos.groupby(['stock_symbol', 'stock_name']).agg(
                    ETF家數=('source_etf', 'count'), 總加碼權重=('diff_weight', 'sum'), 買進的ETF清單=('source_etf', lambda x: ', '.join(x))
                ).reset_index().sort_values(by=['ETF家數', '總加碼權重'], ascending=[False, False]).reset_index(drop=True)
                
                df_grouped_pos = df_grouped_pos.rename(columns={'stock_symbol': '股票代號', 'stock_name': '股票名稱'})
                st.dataframe(
                    df_grouped_pos, use_container_width=True, hide_index=True,
                    column_config={
                        '股票代號': st.column_config.TextColumn("代號", width="small"),
                        '股票名稱': st.column_config.TextColumn("名稱", width="small"),
                        'ETF家數': st.column_config.ProgressColumn("聯手買進家數", format="%d 家", min_value=0, max_value=max(int(df_grouped_pos['ETF家數'].max()), 1)),
                        '總加碼權重': st.column_config.NumberColumn("總增幅權重 (%)", format="%.2f%%"),
                        '買進的ETF清單': st.column_config.TextColumn("參與買進的 ETF清單", width="medium")
                    }
                )
            else:
                st.info("近期沒有出現個股加碼的動作。")
                
            st.markdown("---")
            st.subheader("🧊 投信聯手減碼 (拋售) 共識")
            if consensus_negative:
                df_neg = pd.DataFrame(consensus_negative)
                df_grouped_neg = df_neg.groupby(['stock_symbol', 'stock_name']).agg(
                    ETF家數=('source_etf', 'count'), 總減碼權重=('diff_weight', 'sum'), 賣出的ETF清單=('source_etf', lambda x: ', '.join(x))
                ).reset_index().sort_values(by=['ETF家數', '總減碼權重'], ascending=[False, False]).reset_index(drop=True)
                
                df_grouped_neg = df_grouped_neg.rename(columns={'stock_symbol': '股票代號', 'stock_name': '股票名稱'})
                st.dataframe(
                    df_grouped_neg, use_container_width=True, hide_index=True,
                    column_config={
                        '股票代號': st.column_config.TextColumn("代號", width="small"),
                        '股票名稱': st.column_config.TextColumn("名稱", width="small"),
                        'ETF家數': st.column_config.ProgressColumn("聯手拋售家數", format="%d 家", min_value=0, max_value=max(int(df_grouped_neg['ETF家數'].max()), 1)),
                        '總減碼權重': st.column_config.NumberColumn("總砍單權重 (%)", format="-%.2f%%"),
                        '賣出的ETF清單': st.column_config.TextColumn("參與賣出的 ETF清單", width="medium")
                    }
                )
            else:
                st.info("近期沒有出現個股減碼的動作。")
        else:
            st.info("雲端資料庫目前尚無可用歷史紀錄來進行比對計算。")
