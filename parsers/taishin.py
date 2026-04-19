import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from parsers.base import BaseParser

class TaishinParser(BaseParser):
    """
    台新投信 (Taishin) 持股解析器
    """
    
    def validate(self) -> bool:
        """測試連線能力"""
        return True
    
    def fetch_data(self, date_str: str) -> pd.DataFrame:
        # 官網網址格式: https://www.tsit.com.tw/ETF/Home/Pcf/00936?FundType=ALL&DataDate=2024-04-17
        url = f"https://www.tsit.com.tw/ETF/Home/Pcf/{self.ticker}"
        params = {
            "FundType": "ALL",
            "DataDate": date_str
        }
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=15)
            if response.status_code != 200:
                return pd.DataFrame()
                
            soup = BeautifulSoup(response.text, 'lxml')
            
            # 台新投信頁面有多個表格，股票表格標題有「股數」，債券表格標題有「面額」
            target_table = None
            for table in soup.find_all('table'):
                header_text = table.text
                if '代號' in header_text and ('股數' in header_text or '面額' in header_text):
                    target_table = table
                    break
            
            if not target_table:
                return pd.DataFrame()
                
            rows = target_table.find_all('tr')
            data_list = []
            
            # 尋找標頭索引 (使用模糊匹配)
            header_row = rows[0].find_all(['th', 'td'])
            headers = [h.text.strip() for h in header_row]
            
            idx_symbol, idx_name, idx_shares, idx_weight = -1, -1, -1, -1
            for i, h in enumerate(headers):
                if '代號' in h or '代碼' in h: idx_symbol = i
                elif '名稱' in h: idx_name = i
                elif '股數' in h or '面額' in h: idx_shares = i
                elif '權重' in h: idx_weight = i
            
            # 如果還是找不到關鍵索引，使用預設 0, 1, 2, 3
            if -1 in [idx_symbol, idx_name, idx_shares, idx_weight]:
                idx_symbol, idx_name, idx_shares, idx_weight = 0, 1, 2, 3
            
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) <= max(idx_symbol, idx_name, idx_shares, idx_weight):
                    continue
                
                # 處理代號: 台新的格式可能是 "2330 TT"，要去掉 " TT" 或 ".TT"
                raw_symbol = cols[idx_symbol].text.strip()
                stock_symbol = raw_symbol.split(' ')[0].split('.')[0]
                stock_name = cols[idx_name].text.strip()
                
                # 處理數值
                shares_str = cols[idx_shares].text.replace(',', '').strip()
                weight_str = cols[idx_weight].text.replace('%', '').strip()
                
                try:
                    shares = int(float(shares_str))
                    weight = float(weight_str)
                    
                    if stock_symbol and stock_name:
                        data_list.append({
                            "ticker": self.ticker,
                            "date": date_str,
                            "stock_symbol": stock_symbol,
                            "stock_name": stock_name,
                            "shares": shares,
                            "weight": weight
                        })
                except ValueError:
                    continue
                    
            return pd.DataFrame(data_list)
            
        except Exception as e:
            print(f"TaishinParser Error on {date_str}: {e}")
            
        return pd.DataFrame()
