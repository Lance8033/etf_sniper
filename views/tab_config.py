import streamlit as st
from core.database import get_all_etfs, add_etf_config, update_etf_config_status, delete_etf_config
from core.twse import fetch_twse_etf_name
from parsers.factory import get_parser, PARSER_REGISTRY, populate_mock_history_for_ticker

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

def render():
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
                                sync_etf_history(new_ticker, new_name, new_issuer, new_parser)
                                st.success(f"成功: {new_ticker} {new_name} 已加入雲端偵察序列並完成初步數據同步！")
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
