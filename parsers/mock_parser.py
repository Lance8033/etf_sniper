import pandas as pd
from parsers.base import BaseParser

class MockHtmlTableParser(BaseParser):
    """
    這是一個暫時的佔位符解析器。
    """
    def validate(self):
        return True
        
    def fetch_data(self, date: str) -> pd.DataFrame:
        # Debug Log
        print(f"DEBUG: MockHtmlTableParser called for {self.ticker} ({self.issuer})")
        return pd.DataFrame()

class MockCsvDownloadParser(BaseParser):
    """
    這是一個暫時的佔位符解析器。
    針對尚未實作真實爬蟲的投信，系統將會回傳空資料。
    """
    def validate(self):
        return True
        
    def fetch_data(self, date: str) -> pd.DataFrame:
        return pd.DataFrame()
