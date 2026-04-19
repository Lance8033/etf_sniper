import requests
import datetime
import pandas as pd
from parsers.base import BaseParser

class CapitalParser(BaseParser):
    """
    群益投信 (Capital) 持股解析器
    使用內部 JSON API
    """
    
    # 群益 ETF 代號與內部 ID 的映射
    CAPITAL_ID_MAP = {
        "00919": 195,  # 群益台灣精選高息
        "00946": 388,  # 群益科技高息成長
        "00923": 365,  # 群益台ESG低碳50
        "00992A": 500, # 主動群益科技創新
        "00937B": 378, # 群益ESG投等債20+
        "00953B": 389, # 群益優選非投等債
        "00927": 366,  # 群益半導體收益
        "00764B": 175, # 群益25年美債
        "00720B": 105, # 群益投資級10年+公司債
        "00722B": 103, # 群益投資級電信債
        "00982A": 399, # 主動群益台灣強棒
        "00997A": 502, # 主動群益美國增長
    }
    
    def validate(self) -> bool:
        """測試連線能力"""
        return True
    
    def fetch_data(self, date_str: str) -> pd.DataFrame:
        url = "https://www.capitalfund.com.tw/CFWeb/api/etf/buyback"
        
        # 診斷：確認輸入
        print(f"[DEBUG] CapitalParser START for ticker: '{self.ticker}'")
        
        fund_id = self.CAPITAL_ID_MAP.get(self.ticker)
        if not fund_id:
            print(f"[DEBUG] CapitalParser: Ticker {self.ticker} not found in MAP!")
            return pd.DataFrame()
            
        # 群益的 API 接受 POST 請求，payload 為 {"fundId": str(fund_id), "date": None}
        payload = {
            "fundId": str(fund_id),
            "date": None
        }
        
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        print(f"[DEBUG] CapitalParser Payload: {payload}")
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=12)
            print(f"[DEBUG] CapitalParser Response Status: {response.status_code}")
            
            if response.status_code != 200:
                print(f"Capital API Error: {response.status_code} | Body: {response.text[:200]}")
                return pd.DataFrame()
                
            json_data = response.json()
            
            # 群益的結構可能在 data.pcfList 或 data.stocks
            data_content = json_data.get('data')
            if isinstance(data_content, dict):
                pcf_list = data_content.get('pcfList') or data_content.get('stocks') or []
            elif isinstance(data_content, list):
                pcf_list = data_content
            else:
                pcf_list = json_data.get('pcfList') or json_data.get('stocks') or []
            
            if not pcf_list:
                return pd.DataFrame()
                
            data_list = []
            for item in pcf_list:
                if not isinstance(item, dict):
                    continue
                    
                # 欄位依據官網 API 結構 (stocNo, stocName, weight)
                stock_symbol = str(item.get('stocNo') or item.get('stockCode') or item.get('code') or '').strip()
                stock_name = item.get('stocName') or item.get('stockName') or item.get('name') or ''
                
                try:
                    # 去除逗號後轉換
                    share_raw = str(item.get('share') or item.get('shares') or '0').replace(',', '')
                    weight_raw = str(item.get('weight') or item.get('ratio') or item.get('navRate') or '0').replace(',', '').replace('%', '')
                    
                    shares = int(float(share_raw))
                    weight = float(weight_raw)
                    
                    if stock_symbol and stock_name:
                        data_list.append({
                            "ticker": self.ticker,
                            "date": date_str,
                            "stock_symbol": stock_symbol,
                            "stock_name": stock_name,
                            "shares": shares,
                            "weight": weight
                        })
                except (ValueError, TypeError):
                    continue
            
            return pd.DataFrame(data_list)
            
        except Exception as e:
            print(f"CapitalParser Error on {self.ticker}: {e}")
            
        return pd.DataFrame()
