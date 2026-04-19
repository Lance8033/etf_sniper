import datetime
from parsers.mock_parser import MockHtmlTableParser, MockCsvDownloadParser
from parsers.unipresident import UniPresidentParser
from core.database import supabase, check_db_connection

# Parser Factory Mapping
PARSER_REGISTRY = {
    "網頁表格型 (HTML Table)": MockHtmlTableParser,
    "CSV下載型 (CSV Download)": MockCsvDownloadParser
}

def get_parser(parser_type: str, ticker: str, issuer: str):
    """獲取解析器實例處理路徑"""
    print(f"DEBUG: get_parser called with issuer='{issuer}' (len={len(issuer)})")
    # 優先檢查是否有特定投信的專用解析器
    if issuer.strip() == "統一":
        return UniPresidentParser(ticker, issuer)
        
    parser_class = PARSER_REGISTRY.get(parser_type)
    if not parser_class:
        raise ValueError(f"Unknown parser type: {parser_type}")
    return parser_class(ticker, issuer)

def execute_history_sync(ticker, issuer, parser_type, *args, **kwargs):
    """
    同步 ETF 的歷史資料。使用 *args 避免 Streamlit 快取導致的參數數量不一致報錯。
    """
    check_db_connection()
    # 清除舊的歷史紀錄，確保數據純淨
    supabase.table('etf_holdings_history').delete().eq('ticker', ticker).execute()
    
    parser = get_parser(parser_type, ticker, issuer)
    today = datetime.date.today()
    dates = [(today - datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(14, -1, -1)]
    
    for date in dates:
        df = parser.fetch_data(date)
        records = df.to_dict(orient='records')
        if records:
            try:
                supabase.table('etf_holdings_history').upsert(records).execute()
            except Exception:
                pass
