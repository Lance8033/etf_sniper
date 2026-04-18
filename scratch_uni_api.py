import requests
import json
import datetime

def test_unipresident_api():
    url = "https://www.ezmoney.com.tw/ETF/Transaction/GetPCF"
    # Convert today to ROC date (e.g., 2024-04-19 -> 113/04/19)
    # The browser subagent saw "115/04/17" for 2026-04-17.
    now = datetime.datetime.now()
    roc_year = now.year - 1911
    roc_date = f"{roc_year:03d}/{now.month:02d}/{now.day:02d}"
    
    # Try yesterday if today is weekend or early morning
    yesterday = now - datetime.timedelta(days=1)
    roc_year_y = yesterday.year - 1911
    roc_date_y = f"{roc_year_y:03d}/{yesterday.month:02d}/{yesterday.day:02d}"

    payload = {
        "fundCode": "49YTW",
        "date": roc_date_y,
        "specificDate": False
    }
    
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    print(f"Testing with payload: {payload}")
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            # print(json.dumps(data, indent=2, ensure_ascii=False)[:1000])
            assets = data.get('asset', [])
            stock_asset = next((a for a in assets if a.get('AssetName') == '股票'), None)
            if stock_asset:
                details = stock_asset.get('Details', [])
                print(f"Found {len(details)} stocks.")
                for d in details[:5]:
                    print(f" - {d.get('DetailCode')}: {d.get('DetailName')} ({d.get('NavRate')}%)")
            else:
                print("No '股票' asset found in response.")
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    test_unipresident_api()
