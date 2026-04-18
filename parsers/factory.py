import datetime
from parsers.mock_parser import MockHtmlTableParser, MockCsvDownloadParser
from core.database import supabase, check_db_connection

# Parser Factory Mapping
PARSER_REGISTRY = {
    "網頁表格型 (HTML Table)": MockHtmlTableParser,
    "CSV下載型 (CSV Download)": MockCsvDownloadParser
}

def get_parser(parser_type: str, ticker: str, issuer: str):
    """獲取解析器實例"""
    parser_class = PARSER_REGISTRY.get(parser_type)
    if not parser_class:
        raise ValueError(f"Unknown parser type: {parser_type}")
    return parser_class(ticker, issuer)

def populate_mock_history_for_ticker(ticker: str, issuer: str, parser_type: str):
    """此為展示用：當新增 ETF 時，自動產生前五個交易日的歷史資料"""
    check_db_connection()
    parser = get_parser(parser_type, ticker, issuer)
    today = datetime.date.today()
    dates = [(today - datetime.timedelta(days=i)).strftime("%Y-%m-%d") for i in range(5, -1, -1)]
    
    for date in dates:
        df = parser.fetch_data(date)
        records = df.to_dict(orient='records')
        if records:
            try:
                supabase.table('etf_holdings_history').upsert(records).execute()
            except Exception:
                pass
