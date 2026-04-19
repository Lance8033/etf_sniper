import requests
import pandas as pd
import datetime
from parsers.base import BaseParser

class NomuraParser(BaseParser):
    """
    野村投信 (Nomura) 持股解析器
    使用官方 JSON API: GetFundAssets
    """
    
    def validate(self) -> bool:
        """驗證解析器是否可用"""
        return True

    def fetch_data(self, date_str: str) -> pd.DataFrame:
        url = "https://www.nomurafunds.com.tw/API/ETFAPI/api/Fund/GetFundAssets"
        
        # 野村的日期格式為 YYYY-MM-DD (由 date_str 傳入)
        # Payload 使用 FundID 與 SearchDate
        payload = {
            "FundID": self.ticker,
            "SearchDate": date_str
        }
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=15)
            if response.status_code != 200:
                print(f"Nomura API Error: {response.status_code}")
                return pd.DataFrame()
            
            data = response.json()
            # 野村使用 StatusCode == 0 代表成功
            if data.get('StatusCode') != 0:
                print(f"Nomura API Data Status Error: {data.get('Message')}")
                return pd.DataFrame()
            
            # 野村結構: Entries -> Data -> Table (是一個列表，要找 TableTitle == "股票")
            tables = data.get('Entries', {}).get('Data', {}).get('Table', [])
            stock_table = None
            for t in tables:
                if t.get('TableTitle') == "股票":
                    stock_table = t
                    break
            
            if not stock_table:
                print(f"No '股票' table found in Nomura data for {self.ticker}")
                return pd.DataFrame()
            
            # Rows 是列表的列表: [StockCode, StockName, Shares, Weight]
            rows = stock_table.get('Rows', [])
            data_list = []
            for r in rows:
                if len(r) < 4:
                    continue
                
                stock_symbol = str(r[0]).strip()
                stock_name = str(r[1]).strip()
                
                try:
                    # 數值處理: 去除逗號與百分比
                    shares = int(str(r[2]).replace(',', '').split('.')[0])
                    weight = float(str(r[3]).replace('%', '').replace(',', ''))
                    
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
            print(f"Error fetching Nomura data for {self.ticker}: {e}")
            return pd.DataFrame()
