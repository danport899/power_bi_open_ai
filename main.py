

import requests
from dotenv import load_dotenv
import os


load_dotenv()

power_bi_app_id = os.getenv('POWER_BI_APP_ID')
workspace_id = os.getenv('WORKSPACE_ID')
dataset_id = os.getenv('DATASET_ID')

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

def generate_token():
    secrets = {
        "client_id": client_id,
        "client_secret": client_secret,
        'scope' : "https://analysis.windows.net/powerbi/api/.default"
        ,'grant_type' : "client_credentials"
    }
    url = f'https://login.microsoftonline.com/{power_bi_app_id}/oauth2/v2.0/token'

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.request("POST", url, headers=headers, data=secrets)
    token= response.json().get('access_token')
    headers = {"Authorization": f"Bearer {token}", 
            "Content-Type": "application/json"}
    return headers


def refresh_dataset(headers):
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes'

    request = requests.request("POST",url, headers=headers)

    
    return request.status_code



headers = generate_token()

response = refresh_dataset(headers)
print(response)