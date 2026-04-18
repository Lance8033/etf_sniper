from parsers.factory import get_parser
import datetime

def verify_integration():
    ticker = "00981A"
    issuer = "統一"
    parser_type = "網頁表格型 (HTML Table)"
    
    print(f"Testing Factory for {ticker} ({issuer})...")
    parser = get_parser(parser_type, ticker, issuer)
    print(f"Using Parser: {type(parser).__name__}")
    
    if type(parser).__name__ != "UniPresidentParser":
        print("FAIL: Should be UniPresidentParser")
        return

    today = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Fetching data for {today}...")
    df = parser.fetch_data(today)
    
    if not df.empty:
        print(f"SUCCESS: Fetched {len(df)} records.")
        print(df.head())
    else:
        print("FAIL: DataFrame is empty. Check if API is down or date is invalid.")

if __name__ == "__main__":
    verify_integration()
