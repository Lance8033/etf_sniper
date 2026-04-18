import streamlit as st
from core.database import supabase, get_active_etfs
from views import tab_current, tab_diff, tab_trend, tab_consensus, tab_config

# ==========================================
# 0. Page Config & Initialization
# ==========================================
st.set_page_config(page_title="主動 ETF 籌碼狙擊鏡", layout="wide", page_icon="🎯")

st.title("🎯 主動 ETF 籌碼狙擊鏡 - 動態偵察版")
st.markdown("千萬資產免稅複利引擎核心系統 (Cloud Native Edition ☁️)")

# 優先檢查連線配置
if not supabase:
    st.error("🚨 系統偵測到 Supabase 連線尚未設定或失敗。請參考文件設定 `secrets.toml`。")
    st.info("架構已完全升級為 GitHub + Supabase。這代表您不再需要本地資料庫囉！")
    st.stop()

# ==========================================
# 1. Navigation Sidebar
# ==========================================
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/3256/3256903.png", width=50)
st.sidebar.markdown("### 🎯 狙擊偵察目標")
st.sidebar.markdown("---")

active_etfs = get_active_etfs()
if not active_etfs.empty:
    options = (active_etfs['ticker'] + " - " + active_etfs['name']).tolist()
    
    if 'selected_target_etf' not in st.session_state or st.session_state['selected_target_etf'] not in options:
        st.session_state['selected_target_etf'] = options[0]
        
    for opt in options:
        btn_type = "primary" if st.session_state['selected_target_etf'] == opt else "secondary"
        if st.sidebar.button(opt, use_container_width=True, type=btn_type):
            st.session_state['selected_target_etf'] = opt
            st.rerun()
            
    target_etf = st.session_state['selected_target_etf']
    global_target_ticker = target_etf.split(" - ")[0]
else:
    global_target_ticker = None
    target_etf = None

# ==========================================
# 2. Main Content Tabs
# ==========================================
tabs = st.tabs(["📋 最新持股 (Current)", "⚔️ 每日突擊訊號 (Diff)", "📈 時空夜視鏡 (Trend Chart)", "📡 投信共識雷達", "⚙️ 戰略配置中心"])

with tabs[0]:
    tab_current.render(global_target_ticker, target_etf)

with tabs[1]:
    tab_diff.render(global_target_ticker, target_etf)

with tabs[2]:
    tab_trend.render(global_target_ticker, target_etf)

with tabs[3]:
    tab_consensus.render()

with tabs[4]:
    tab_config.render()
