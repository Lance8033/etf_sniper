import streamlit as st
import pandas as pd
from core.database import supabase, get_active_etfs

def render():
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
