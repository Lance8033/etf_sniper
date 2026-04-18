import pandas as pd
from parsers.base import BaseParser

class MockHtmlTableParser(BaseParser):
    """
    這是一個暫時的佔位符解析器。
    為了確保系統數據的真實性，我們已經移除了所有的隨機假資料產生邏輯。
    針對尚未實作真實爬蟲的投信，系統將會回傳空資料。
    """
    def validate(self):
        return True
        
    def fetch_data(self, date: str) -> pd.DataFrame:
        # 已移除假資料，回傳空 DataFrame 待後續實作真實爬蟲
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
