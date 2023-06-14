

import requests
from dotenv import load_dotenv
import os
import pandas as pd
import openai
import psycopg2

load_dotenv()


openai.api_key = os.getenv('OPENAPI_KEY')
power_bi_app_id = os.getenv('POWER_BI_APP_ID')
workspace_id = os.getenv('WORKSPACE_ID')
dataset_id = os.getenv('DATASET_ID')

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')


def create_conn():
    dbname=os.getenv("REDSHIFT_DB_NAME")
    host = os.getenv("HOST")
    port = os.getenv("PORT")
    user = os.getenv("REDSHIFT_USER")
    password = os.getenv("REDSHIFT_PASSWORD")
    conn = psycopg2.connect(dbname=dbname, host=host,
                     port=port, user=user,
                     password=password)
    return conn

def get_table_sample(redshift_conn, table_name):
    df = pd.read_sql_query(
        f"""
      SELECT * from {table_name} LIMIT 10
        """, redshift_conn
        )
    return df.to_string(index=False)

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


def test_prompt(table_name, df_string):

    url = 'https://api.openai.com/v1/completions'
 
    table_name = 'public.daily_campaigns_fact'
    response = openai.Completion.create(
        engine='text-davinci-003',  # Specify the appropriate engine
        prompt=f"""
        The table {table_name} collects campaign performance across multiple platforms. 
        The campaign_name column represents a campaign.
        Ignoring null and 0 sum campaigns, write a Redshift-valid SQL query that determines 
        which 10 campaigns have the highest clickthrough rate based on this csv: {df_string}
        """,
        max_tokens=400  # Adjust bas
    )
   
    response = response['choices'][0]['text']
    response = response.replace(';','')
    return response

table_name = 'public.daily_campaigns_fact'
new_table = 'dbt_dportuondo_final.open_ai_citrus_ad_template'
redshift_conn = create_conn()

df_string = get_table_sample(redshift_conn, table_name)

openai_reponse = test_prompt(table_name, df_string)


cur = redshift_conn.cursor()

cur.execute(f'DROP TABLE IF EXISTS {new_table};')

print(f'CREATE TABLE {new_table} AS ({openai_reponse});')
cur.execute(f'CREATE TABLE {new_table} AS ({openai_reponse});')

redshift_conn.commit()
