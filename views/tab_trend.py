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
