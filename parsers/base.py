import pandas as pd
import random

class BaseParser:
    """解析器基底介面"""
    def __init__(self, ticker: str, issuer: str):
        self.ticker = ticker
        self.issuer = issuer
        
    def validate(self) -> bool:
        """實作：測試連線與解析能力，供新增配置時阻擋無效 Parser"""
        raise NotImplementedError

    def fetch_data(self, date: str) -> pd.DataFrame:
        """實作：抓取指定日期的籌碼"""
        raise NotImplementedError
