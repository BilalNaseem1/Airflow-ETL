# from airflow import DAG
# from airflow.providers.http.hooks.http import HttpHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.decorators import task
# from airflow.utils.dates import days_ago
# import pandas as pd
# import json
# import requests
# from airflow.decorators import task
# from datetime import datetime

# POSTGRES_CONN_ID = 'postgres_default'
# # API_CONN_ID = '4eaeaf12-57a0-48a9-886b-881aa9f856c7'
# API_CONN_ID = 'coinmarketcap_api'

# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1)
# }

# with DAG(dag_id='coinmarketcap_etl_pipeline',
#          default_args=default_args,
#          schedule_interval='@hourly',
#          catchup=False) as dag:
         
#         @task()
#         def extract_crypto_data():
#             url = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing'
#             headers = {
#                 'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0',
#                 'Accept': 'application/json, text/plain, */*',
#                 'Accept-Language': 'en-CA,en-US;q=0.7,en;q=0.3',
#                 'Accept-Encoding': 'gzip, deflate, br, zstd',
#                 'Referer': 'https://coinmarketcap.com/',
#                 'x-request-id': 'd04b8eb8ee984c7895944ca3bf2ccc62',
#                 'platform': 'web',
#                 'cache-control': 'no-cache',
#                 'Origin': 'https://coinmarketcap.com',
#                 'Connection': 'keep-alive',
#                 'Sec-Fetch-Dest': 'empty',
#                 'Sec-Fetch-Mode': 'cors',
#                 'Sec-Fetch-Site': 'same-site',
#                 'TE': 'trailers'
#                 }
#             params = {
#                 'start': 1,
#                 'limit': 300,
#                 'sortBy': 'rank',
#                 'sortType': 'desc',
#                 'convert': 'USD,BTC,ETH',
#                 'cryptoType': 'all',
#                 'tagType': 'all',
#                 'audited': 'false',
#                 'aux': 'ath,atl,high24h,low24h,num_market_pairs,cmc_rank,date_added,max_supply,circulating_supply,total_supply,volume_7d,volume_30d,self_reported_circulating_supply,self_reported_market_cap,quotes'
#                 }

#             response = requests.get(url, headers=headers, params=params)
            
#             if response.status_code == 200:
#                 return response.json()
#             else:
#                 raise Exception(f"Failed to fetch cryptocurrency data: {response.status_code}")


#         @task()
#         def transform_crypto_data(raw_data):
#             if 'data' in raw_data and 'cryptoCurrencyList' in raw_data['data']:
#                 df = pd.json_normalize(raw_data['data']['cryptoCurrencyList'])
                
#                 coins = df[['symbol', 'cmcRank', 'marketPairCount', 'quotes']]
                
#                 coins['volume24h'] = coins['quotes'].apply(lambda x: x[2].get('volume24h') if isinstance(x[2], dict) else None)
#                 coins['volume7d'] = coins['quotes'].apply(lambda x: x[2].get('volume7d') if isinstance(x[2], dict) else None)
#                 coins['marketCap'] = coins['quotes'].apply(lambda x: x[2].get('marketCap') if isinstance(x[2], dict) else None)
#                 coins['vol24h_vs_MC'] = coins['quotes'].apply(lambda x: x[2].get('turnover') if isinstance(x[2], dict) else None)
#                 coins['marketCapByTotalSupply'] = coins['quotes'].apply(lambda x: x[2].get('marketCapByTotalSupply') if isinstance(x[2], dict) else None)
#                 coins['fullyDilluttedMarketCap'] = coins['quotes'].apply(lambda x: x[2].get('fullyDilluttedMarketCap') if isinstance(x[2], dict) else None)
#                 coins['percentChange1h'] = coins['quotes'].apply(lambda x: x[2].get('percentChange1h') if isinstance(x[2], dict) else None)
#                 coins['percentChange24h'] = coins['quotes'].apply(lambda x: x[2].get('percentChange24h') if isinstance(x[2], dict) else None)
#                 coins['percentChange7d'] = coins['quotes'].apply(lambda x: x[2].get('percentChange7d') if isinstance(x[2], dict) else None)
#                 coins['dominance'] = coins['quotes'].apply(lambda x: x[2].get('dominance') if isinstance(x[2], dict) else None)
#                 coins['percentChange24h_vs_BTC'] = coins['quotes'].apply(lambda x: x[0].get('percentChange24h') if isinstance(x[0], dict) else None)
#                 coins['percentChange7d_vs_BTC'] = coins['quotes'].apply(lambda x: x[0].get('percentChange7d') if isinstance(x[0], dict) else None)
#                 coins['percentChange30d_vs_BTC'] = coins['quotes'].apply(lambda x: x[0].get('percentChange30d') if isinstance(x[0], dict) else None)
#                 coins['percentChange60d_vs_BTC'] = coins['quotes'].apply(lambda x: x[0].get('percentChange60d') if isinstance(x[0], dict) else None)
#                 coins['percentChange90d_vs_BTC'] = coins['quotes'].apply(lambda x: x[0].get('percentChange90d') if isinstance(x[0], dict) else None)
                
#                 coins = coins.drop(columns=['quotes'])
#                 return coins.to_dict(orient='records')
#             else:
#                 raise ValueError("Invalid data structure received from API")

        

#         @task()
#         def load_crypto_data(transformed_data):
#             pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
#             conn = pg_hook.get_conn()
#             cursor = conn.cursor()

#             # Get the current timestamp
#             current_timestamp = datetime.now()

#             # Ensure the table exists
#             cursor.execute("""
#             CREATE TABLE IF NOT EXISTS crypto_historical_data (
#                 id SERIAL PRIMARY KEY,
#                 symbol TEXT NOT NULL,
#                 cmc_rank INT,
#                 market_pair_count INT,
#                 volume24h FLOAT,
#                 volume7d FLOAT,
#                 market_cap FLOAT,
#                 vol24h_vs_mc FLOAT,
#                 market_cap_by_total_supply FLOAT,
#                 fully_dillutted_market_cap FLOAT,
#                 percent_change_1h FLOAT,
#                 percent_change_24h FLOAT,
#                 percent_change_7d FLOAT,
#                 dominance FLOAT,
#                 percent_change_24h_vs_btc FLOAT,
#                 percent_change_7d_vs_btc FLOAT,
#                 percent_change_30d_vs_btc FLOAT,
#                 percent_change_60d_vs_btc FLOAT,
#                 percent_change_90d_vs_btc FLOAT,
#                 data_timestamp TIMESTAMP NOT NULL,
#                 inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#                 UNIQUE (symbol, data_timestamp) -- Prevent duplicate entries for the same symbol and timestamp
#             );
#             """)

#             for record in transformed_data:
#                 # Explicitly add the current timestamp to each record
#                 record['data_timestamp'] = current_timestamp

#                 cursor.execute("""
#                 INSERT INTO crypto_historical_data (
#                     symbol, cmc_rank, market_pair_count, volume24h, volume7d, 
#                     market_cap, vol24h_vs_mc, market_cap_by_total_supply, 
#                     fully_dillutted_market_cap, percent_change_1h, percent_change_24h, 
#                     percent_change_7d, dominance, percent_change_24h_vs_btc, 
#                     percent_change_7d_vs_btc, percent_change_30d_vs_btc, 
#                     percent_change_60d_vs_btc, percent_change_90d_vs_btc, data_timestamp
#                 ) VALUES (
#                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
#                 )
#                 ON CONFLICT (symbol, data_timestamp) DO NOTHING; -- Avoid duplicates
#                 """, (
#                     record['symbol'], record['cmcRank'], record['marketPairCount'], 
#                     record['volume24h'], record['volume7d'], record['marketCap'], 
#                     record['vol24h_vs_MC'], record['marketCapByTotalSupply'], 
#                     record['fullyDilluttedMarketCap'], record['percentChange1h'], 
#                     record['percentChange24h'], record['percentChange7d'], 
#                     record['dominance'], record['percentChange24h_vs_BTC'], 
#                     record['percentChange7d_vs_BTC'], record['percentChange30d_vs_BTC'], 
#                     record['percentChange60d_vs_BTC'], record['percentChange90d_vs_BTC'], 
#                     record['data_timestamp']  # Use the current timestamp
#                 ))

#             conn.commit()
#             cursor.close()

#             # DAG Workflow
#             raw_data = extract_crypto_data()
#             transformed_data = transform_crypto_data(raw_data)
#             load_crypto_data(transformed_data)