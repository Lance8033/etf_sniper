import pandas as pd
import random
from parsers.base import BaseParser

class MockHtmlTableParser(BaseParser):
    """臨時使用的假資料生成器"""
    def validate(self):
        return True # 假裝置驗證成功
        
    def fetch_data(self, date: str) -> pd.DataFrame:
        # Mocking data specifically for demonstration
        return self._generate_mock_data(date)
        
    def _generate_mock_data(self, date: str) -> pd.DataFrame:
        stocks = [("2330", "台積電"), ("2317", "鴻海"), ("2454", "聯發科"), ("2308", "台達電"), ("3008", "大立光")]
        # 利用日期的單雙數來動態模擬「新增建倉」與「全數清倉」
        day = int(date.split('-')[-1])
        if day % 2 == 0:
            stocks.append(("2382", "廣達")) # 偶數日有廣達
        else:
            stocks.append(("2303", "聯電")) # 奇數日有聯電
            
        data = []
        for sym, name in stocks:
            # 加上一點隨機雜訊，模擬每日權重浮動
            random_weight = round(random.uniform(2.0, 15.0), 2)
            random_shares = int(random_weight * 1000)
            data.append({
                "ticker": self.ticker,
                "date": date,
                "stock_symbol": sym,
                "stock_name": name,
                "shares": random_shares,
                "weight": random_weight
            })
        return pd.DataFrame(data)

class MockCsvDownloadParser(BaseParser):
    def validate(self):
        return True
        
    def fetch_data(self, date: str) -> pd.DataFrame:
        return MockHtmlTableParser(self.ticker, self.issuer).fetch_data(date)
