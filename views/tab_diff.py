import streamlit as st
import pandas as pd
from core.database import supabase

def render(global_target_ticker, target_etf):
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
