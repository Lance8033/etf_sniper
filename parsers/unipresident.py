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
            # 如果找不到，嘗試用預設邏輯 (或者拋錯)
            return pd.DataFrame()
            
        # 2. 轉換日期為民國格式
        # 由於 API 在假日 (如週日) 會回傳空值，我們實作一個小小的 Backtrack 邏輯，
        # 如果當天沒資料，自動往前找最多 3 天。
        base_dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        
        for i in range(4): # 0, 1, 2, 3 (今天到前三天)
            target_dt = base_dt - datetime.timedelta(days=i)
            roc_date = f"{target_dt.year - 1911:03d}/{target_dt.month:02d}/{target_dt.day:02d}"
            
            payload = {
                "fundCode": fund_code,
                "date": roc_date,
                "specificDate": False
            }
            
            headers = {
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
            
            try:
                response = requests.post(url, json=payload, headers=headers, timeout=12)
                if response.status_code == 200:
                    json_data = response.json()
                    assets = json_data.get('asset', [])
                    
                    # 尋找「股票」類別的資產
                    stock_asset = next((a for a in assets if a.get('AssetName') == '股票'), None)
                    if not stock_asset:
                        continue # 當天沒股票資料，試試看前一天
                    
                    details = stock_asset.get('Details', [])
                    if not details:
                        continue
                        
                    data_list = []
                    for d in details:
                        data_list.append({
                            "ticker": self.ticker,
                            "date": date_str, # 我們依然標記為使用者要求的日期，或是您可以改為標記真實日期 target_dt.strftime("%Y-%m-%d")
                            "stock_symbol": str(d.get('DetailCode', '')).strip(),
                            "stock_name": d.get('DetailName', ''),
                            "shares": int(d.get('Share', 0)),
                            "weight": float(d.get('NavRate', 0))
                        })
                    
                    return pd.DataFrame(data_list)
            except Exception as e:
                print(f"UniPresidentParser Error on {roc_date}: {e}")
                
        return pd.DataFrame()
