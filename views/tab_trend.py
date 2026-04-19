import streamlit as st
import pandas as pd
import plotly.express as px
from core.database import supabase

def render(global_target_ticker, target_etf):
    st.header("📈 時空夜視鏡 - 籌碼趨勢雷達")
    
    if not global_target_ticker:
        st.warning("👈 請先從左側選單選擇偵察目標。")
    else:
        st.markdown(f"**目前鎖定目標**: {target_etf}")
        
        # 1. 抓取完整歷史數據
        response = supabase.table('etf_holdings_history').select('date, stock_symbol, stock_name, shares, weight').eq('ticker', global_target_ticker).order('date').execute()
        df_all = pd.DataFrame(response.data)
        
        if not df_all.empty:
            # 2. 維度切換控制項
            view_mode = st.radio(
                "選擇觀察維度 (View Mode)", 
                ["🛡️ 權重視角 (%)", "🎯 實彈視角 (張數)", "🔥 主動加碼 (Active Move %)"],
                horizontal=True
            )
            
            # 3. 預處理：建立顯示標籤與計算每日期基金總股數 (用於計算 Active Move)
            df_all['display_label'] = df_all['stock_symbol'] + " " + df_all['stock_name']
            
            # 計算每日總股數 (Proxy for Fund Size Expansion)
            daily_total = df_all.groupby('date')['shares'].sum().reset_index()
            daily_total.rename(columns={'shares': 'fund_total_shares'}, inplace=True)
            df_all = df_all.merge(daily_total, on='date')
            
            # 計算 Active Move 邏輯
            # 我們需要對每一檔股票計算 t / t-1 的變動率，再除以基金整體的 t / t-1 變動率
            df_all = df_all.sort_values(['stock_symbol', 'date'])
            df_all['prev_shares'] = df_all.groupby('stock_symbol')['shares'].shift(1)
            df_all['prev_fund_total'] = df_all.groupby('stock_symbol')['fund_total_shares'].shift(1)
            
            # 只有當昨天也有資料時才計算變動率
            df_all['active_move'] = (
                (df_all['shares'] / df_all['prev_shares']) / 
                (df_all['fund_total_shares'] / df_all['prev_fund_total']) - 1
            ) * 100 # 轉為百分比
            
            # 填充 NaN (第一天) 為 0
            df_all['active_move'] = df_all['active_move'].fillna(0)
            
            # 4. 自動挑選權重前三名作為預設值
            latest_date = df_all['date'].max()
            top_3_labels = df_all[df_all['date'] == latest_date].sort_values('weight', ascending=False)['display_label'].head(3).tolist()
            
            labels_available = sorted(df_all['display_label'].unique())
            selected_labels = st.multiselect("選擇欲觀察的成份股", labels_available, default=top_3_labels)
            
            if selected_labels:
                plot_data = df_all[df_all['display_label'].isin(selected_labels)]
                
                # 根據模式決定 Y 軸與單位
                y_col = 'weight'
                y_label = '權重 (%)'
                title_suffix = '權重趨勢'
                
                if "張數" in view_mode:
                    plot_data['shares_cnt'] = plot_data['shares'] / 1000 # 轉為張
                    y_col = 'shares_cnt'
                    y_label = '持股張數 (張)'
                    title_suffix = '持股張數趨勢'
                elif "Active Move" in view_mode:
                    y_col = 'active_move'
                    y_label = '主動加碼幅度 (%)'
                    title_suffix = '主動加碼 (Active Move) 趨勢'
                
                fig = px.line(plot_data, x='date', y=y_col, color='display_label', markers=True,
                              title=f'{global_target_ticker} - {title_suffix}',
                              labels={'date': '日期', y_col: y_label, 'display_label': '個股'})
                
                fig.update_layout(hovermode="x unified", template='plotly_dark')
                # 加入一條 0 軸線給 Active Move 使用
                if "Active Move" in view_mode:
                    fig.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="被動持平線")
                
                st.plotly_chart(fig, use_container_width=True)
                
                if "Active Move" in view_mode:
                    st.caption("ℹ️ **Active Move 解讀**：0% 以上代表加碼幅度超越基金規模成長；0% 以下則代表加碼幅度落後基金成長（可能是主動減碼或被動稀釋）。")
            else:
                st.info("請於上方選單選擇至少一檔成份股。")
        else:
            st.info(f"目前雲端無 {global_target_ticker} 歷史趨勢數據。")

