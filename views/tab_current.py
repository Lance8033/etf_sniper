import streamlit as st
import pandas as pd
from core.database import supabase

def render(global_target_ticker, target_etf):
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
