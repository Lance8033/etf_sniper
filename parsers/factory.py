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
    """獲取解析器實例"""
    # 優先檢查是否有特定投信的專用解析器
    if issuer == "統一":
        return UniPresidentParser(ticker, issuer)
        
    parser_class = PARSER_REGISTRY.get(parser_type)
    if not parser_class:
        raise ValueError(f"Unknown parser type: {parser_type}")
    return parser_class(ticker, issuer)

def populate_mock_history_for_ticker(ticker: str, issuer: str, parser_type: str):
    """此為展示用：當新增 ETF 時，自動產生前五個交易日的歷史資料"""
    check_db_connection()
    # 既然是重新 Populate，為了避免舊的假資料殘留，我們先清除該代號的所有歷史紀錄
    supabase.table('etf_holdings_history').delete().eq('ticker', ticker).execute()
    
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
