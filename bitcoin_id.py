import requests

# Obtain the list of coins from the API
res = requests.get("https://api.coingecko.com/api/v3/coins/list?include_platform=false").json()

# Select the one with name = Bitcoin and print the id
res = [record for record in res if record['name'] == "Bitcoin"]
print(res[0]['id'])