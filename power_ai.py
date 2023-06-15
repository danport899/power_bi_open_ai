
import os, argparse
import requests
from dotenv import load_dotenv, find_dotenv
import pandas as pd
import openai, json, ast
import psycopg2
import s3_helpers

load_dotenv(find_dotenv())

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--type", choices=['text', 'sql'], help="type of answer you'd like to recieve")
parser.add_argument("-q", "--question", help="question for OpenAI")
args = parser.parse_args()
user_prompt_type = args.type
user_prompt = args.question

openai.api_key = os.getenv('OPENAPI_KEY')

power_bi_app_id = os.getenv('POWER_BI_APP_ID')
workspace_id = os.getenv('WORKSPACE_ID')
dataset_id = os.getenv('DATASET_ID')

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

bucket = os.getenv("s3_bucket")

secrets = {
    "client_id": client_id,
    "client_secret": client_secret,
    'scope' : "https://analysis.windows.net/powerbi/api/.default"
    ,'grant_type' : "client_credentials"
}

url = f'https://login.microsoftonline.com/{power_bi_app_id}/oauth2/v2.0/token'

response = requests.request("POST", url, headers={'Content-Type': 'application/x-www-form-urlencoded'}, data=secrets)
token= response.json().get('access_token')
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def initiate_workspace_scan(headers):
    print(f'Initiating Scan of Blackbird workspace')
    url = 'https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo?datasetSchema=true'
    body = {"workspaces": [workspace_id]}
    start_scan_response = requests.request("POST",url, json=body, headers=headers)
    scan_json = start_scan_response.json()
    scan_id = scan_json["id"]
    return scan_id

def await_scan_completion(headers, scan_id):
    print(f'Awaiting completion of Blackbird scan')
    # Check for scan completion every 5 seconds for 30 seconds, otherwise throw an error
    url = f'https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}'
    check_scan_response = requests.request("GET",url, headers=headers)
    status_json = check_scan_response.json()
    if status_json['status'] == 'Succeeded':
        return True
    else:
        return Exception

def get_scan_results(headers, scan_id):
    url = f'https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scan_id}'
    scan_result_response = requests.request("GET",url,headers=headers)
    scan_result = scan_result_response.json()
    scan_result_df = pd.DataFrame(scan_result['workspaces'])
    return scan_result_df

def get_workspace_scan(headers, scan_id):
    if await_scan_completion(headers, scan_id):
        scan_result_df = get_scan_results(headers, scan_id)
        s3_helpers.save_latest_to_s3(scan_result_df, 'power_bi/scanResults','scan_results', bucket=bucket)
        return True

def get_datasets():
    dataset_df = normalize_column('power_bi/scanResults/table=scan_results/scan_results.csv','datasets')
    dataset_df = dataset_df.rename(columns={"name": "dataset_name","id":"dataset_id"})
    s3_helpers.save_latest_to_s3(dataset_df, 'power_bi/datasets','datasets', bucket=bucket)
    return dataset_df

def normalize_column(file_path, target_column):
    df = s3_helpers.download_csv_from_s3(file_path, bucket)
    df.fillna('[]', inplace=True)
    column_dict = df[target_column].apply(lambda x : ast.literal_eval(x))
    column_json = column_dict.apply(lambda x: json.dumps(x))
    column_json = column_json.apply(lambda x: json.loads(x))
    column_json = column_json.explode()
    df=pd.json_normalize(column_json)
    return df

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
    print(f'Checking for {table_name}')
    df = pd.read_sql_query(f"""SELECT * from {table_name} LIMIT 10""", redshift_conn)
    print(f'{df}')
    return df.to_string(index=False)

def generate_token():
    secrets = {
        "client_id": client_id,
        "client_secret": client_secret,
        'scope' : "https://analysis.windows.net/powerbi/api/.default",
        'grant_type' : "client_credentials"
    }
    
    url = f'https://login.microsoftonline.com/{power_bi_app_id}/oauth2/v2.0/token'

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.request("POST", url, headers=headers, data=secrets)
    token= response.json().get('access_token')
    headers = {"Authorization": f"Bearer {token}", 
            "Content-Type": "application/json"}
    
    return headers

def refresh_dataset(headers):
    url = f'https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes'
    request = requests.request("POST",url, headers=headers)
    return request.status_code

def test_prompt(user_prompt, table_name, df_string):
    
    print(f'Asking OpenAI a {user_prompt_type} question: {user_prompt} from {table_name}')
 
    if user_prompt_type == 'sql':
        user_prompt += " only provide a redshift-based SQL query that answers this question"

    response = openai.Completion.create(
        engine='text-davinci-003',
        prompt=f"""{user_prompt} from {table_name} based on {df_string}""",
        max_tokens=400
    )
   
    response = response['choices'][0]['text']
    response = response.replace(';','')
    return response
    
    
scan_id = initiate_workspace_scan(headers)
await_scan_completion(headers, scan_id)
get_workspace_scan(headers, scan_id)
#future-state: use dataset_id of current dataset and only scan tables in that
dataset_df = get_datasets()
dataset = dataset_df['tables']

for tables in dataset:
    for table in tables:

        #assuming that tables in dbt_final and named the same...
        schema = 'dbt_final'
        table_name = schema + '.' + table.get("name")

        redshift_conn = create_conn()

        try:
            df_string = get_table_sample(redshift_conn, table_name)
            openai_reponse = test_prompt(user_prompt, table_name, df_string)

            if user_prompt_type == 'sql':
                cur = redshift_conn.cursor()
                new_table = 'dbt_alyman_final.open_ai_template_' + table.get("name")
                cur.execute(f'DROP TABLE IF EXISTS {new_table};')
                print(f'{openai_reponse}')
                cur.execute(f'CREATE TABLE {new_table} AS ({openai_reponse});') #parse for sql query only
                redshift_conn.commit()
            else:
                print(f'Answer: {openai_reponse}')
        except:
            print('The table does not exist in this schema.')
            continue
