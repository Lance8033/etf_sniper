import requests, json
res = requests.get('https://openapi.twse.com.tw/v1/opendata/t187ap14_L')
with open('debug_twse.json', 'w', encoding='utf-8') as f:
    json.dump(res.json()[:3], f, ensure_ascii=False)
