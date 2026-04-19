import requests
import datetime
import pandas as pd
from parsers.base import BaseParser

class UniPresidentParser(BaseParser):
    """
    統一投信 (Uni-President) 持股解析器
    API 來源: ezmoney.com.tw
    """
    
    # 將 ETF 代號對應到統一投信內部的 fundCode
    FUND_CODE_MAP = {
        "00981A": "49YTW",  # 統一台股增長主動式ETF
        "00931B": "42YTW",  # 統一美債20年 (舉例，暫未驗證)
    }

    def _get_roc_date(self, date_str: str) -> str:
        """將 YYYY-MM-DD 轉換為 民國紀年 yyy/mm/dd"""
        try:
            dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
            roc_year = dt.year - 1911
            return f"{roc_year:03d}/{dt.month:02d}/{dt.day:02d}"
        except Exception:
            return ""

    def validate(self) -> bool:
        # 只要能連上 API 就算驗證成功
        return True

    def fetch_data(self, date_str: str) -> pd.DataFrame:
        url = "https://www.ezmoney.com.tw/ETF/Transaction/GetPCF"
        
        # 1. 查找內部 fundCode
        fund_code = self.FUND_CODE_MAP.get(self.ticker)
        if not fund_code:
            return pd.DataFrame()
            
        # 2. 轉換日期為民國格式
        dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        roc_date = f"{dt.year - 1911:03d}/{dt.month:02d}/{dt.day:02d}"
        
        # 使用 specificDate: True 確保抓到的是精確該日的資料
        payload = {
            "fundCode": fund_code,
            "date": roc_date,
            "specificDate": True
        }
        
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=12)
            if response.status_code == 200:
                json_data = response.json()
                
                # 檢查是否有實際內容 (pcf 裡面有資料才算)
                if not json_data.get('pcf') or not json_data.get('asset'):
                    return pd.DataFrame()

                assets = json_data.get('asset', [])
                
                # 尋找「股票」類別的資產
                stock_asset = next((a for a in assets if a.get('AssetName') == '股票'), None)
                if not stock_asset:
                    return pd.DataFrame()
                
                details = stock_asset.get('Details', [])
                if not details:
                    return pd.DataFrame()
                    
                data_list = []
                for d in details:
                    data_list.append({
                        "ticker": self.ticker,
                        "date": date_str,
                        "stock_symbol": str(d.get('DetailCode', '')).strip(),
                        "stock_name": d.get('DetailName', ''),
                        "shares": int(d.get('Share', 0)),
                        "weight": float(d.get('NavRate', 0))
                    })
                
                return pd.DataFrame(data_list)
        except Exception as e:
            print(f"UniPresidentParser Error on {roc_date}: {e}")
            
        return pd.DataFrame()
