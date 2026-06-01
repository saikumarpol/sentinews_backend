import urllib.request
import json
import socket

url = 'http://127.0.0.1:8000/commodities-dashboard'
print("Querying URL:", url)
try:
    with urllib.request.urlopen(url, timeout=15) as resp:
        data = json.loads(resp.read().decode())
        print("Success! Response status code:", resp.status)
        commodities = data.get('snapshot', {}).get('commodities', [])
        print("Commodities count:", len(commodities))
        if commodities:
            for c in commodities[:5]:
                print(f"- {c['name']} ({c['symbol']}): Price={c['price']}, Change={c['day_change']}, Source={c['source']}")
except urllib.error.URLError as e:
    print("URL Error:", e)
except socket.timeout:
    print("Socket Timeout: The server is taking too long to respond.")
except Exception as e:
    print("General Error:", e)
