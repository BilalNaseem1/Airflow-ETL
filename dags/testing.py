from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from datetime import datetime
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    dag_id='coinmarketcap_etl_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    @task()
    def extract_crypto_data():
        url = 'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing'
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json',
            'Origin': 'https://coinmarketcap.com'
        }
        params = {
            'start': 1,
            'limit': 300,
            'sortBy': 'rank',
            'convert': 'USD,BTC,ETH'
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")

    @task()
    def transform_crypto_data(raw_data):
        df = pd.json_normalize(raw_data['data']['cryptoCurrencyList'])  
        coins = df[['symbol', 'cmcRank', 'marketPairCount', 'quotes']]
        coins['timestamp'] =  datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        coins['volume24h'] = coins['quotes'].apply(lambda x: x[2].get('volume24h') if isinstance(x[2], dict) else None)
        coins['volume7d'] = coins['quotes'].apply(lambda x: x[2].get('volume7d') if isinstance(x[2], dict) else None)
        coins['marketCap'] = coins['quotes'].apply(lambda x: x[2].get('marketCap') if isinstance(x[2], dict) else None)
        coins['vol24h_vs_MC'] = coins['quotes'].apply(lambda x: x[2].get('turnover') if isinstance(x[2], dict) else None)
        coins['marketCapByTotalSupply'] = coins['quotes'].apply(lambda x: x[2].get('marketCapByTotalSupply') if isinstance(x[2], dict) else None)
        coins['fullyDilluttedMarketCap'] = coins['quotes'].apply(lambda x: x[2].get('fullyDilluttedMarketCap') if isinstance(x[2], dict) else None)
        coins['percentChange1h'] = coins['quotes'].apply(lambda x: x[2].get('percentChange1h') if isinstance(x[2], dict) else None)
        coins['percentChange24h'] = coins['quotes'].apply(lambda x: x[2].get('percentChange24h') if isinstance(x[2], dict) else None)
        coins['percentChange7d'] = coins['quotes'].apply(lambda x: x[2].get('percentChange7d') if isinstance(x[2], dict) else None)
        coins['dominance'] = coins['quotes'].apply(lambda x: x[2].get('dominance') if isinstance(x[2], dict) else None)
        coins['percentChange24h_vs_BTC'] = coins['quotes'].apply(lambda x: x[0].get('percentChange24h') if isinstance(x[0], dict) else None)
        coins['percentChange7d_vs_BTC'] = coins['quotes'].apply(lambda x: x[0].get('percentChange7d') if isinstance(x[0], dict) else None)
        coins['percentChange30d_vs_BTC'] = coins['quotes'].apply(lambda x: x[0].get('percentChange30d') if isinstance(x[0], dict) else None)
        coins['percentChange60d_vs_BTC'] = coins['quotes'].apply(lambda x: x[0].get('percentChange60d') if isinstance(x[0], dict) else None)
        coins['percentChange90d_vs_BTC'] = coins['quotes'].apply(lambda x: x[0].get('percentChange90d') if isinstance(x[0], dict) else None)
        
        coins = coins.drop(columns=['quotes'])
        return coins.to_dict(orient='records')

    @task()
    def load_to_postgres(data):
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS crypto_historical (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10),
            cmcRank INTEGER,
            marketPairCount INTEGER,
            timestamp TIMESTAMP,
            volume24h DECIMAL,
            volume7d DECIMAL,
            marketCap DECIMAL,
            vol24h_vs_MC DECIMAL,
            marketCapByTotalSupply DECIMAL,
            fullyDilluttedMarketCap DECIMAL,
            percentChange1h DECIMAL,
            percentChange24h DECIMAL,
            percentChange7d DECIMAL,
            dominance DECIMAL,
            percentChange24h_vs_BTC DECIMAL,
            percentChange7d_vs_BTC DECIMAL,
            percentChange30d_vs_BTC DECIMAL,
            percentChange60d_vs_BTC DECIMAL,
            percentChange90d_vs_BTC DECIMAL
        );
        """
        pg_hook.run(create_table_sql)

        insert_sql = """
        INSERT INTO crypto_historical (
            symbol, cmcRank, marketPairCount, timestamp, volume24h, volume7d,
            marketCap, vol24h_vs_MC, marketCapByTotalSupply, fullyDilluttedMarketCap,
            percentChange1h, percentChange24h, percentChange7d, dominance,
            percentChange24h_vs_BTC, percentChange7d_vs_BTC, percentChange30d_vs_BTC,
            percentChange60d_vs_BTC, percentChange90d_vs_BTC
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        rows = [(
            record['symbol'],
            record['cmcRank'],
            record['marketPairCount'],
            record['timestamp'],
            record['volume24h'],
            record['volume7d'],
            record['marketCap'],
            record['vol24h_vs_MC'],
            record['marketCapByTotalSupply'],
            record['fullyDilluttedMarketCap'],
            record['percentChange1h'],
            record['percentChange24h'],
            record['percentChange7d'],
            record['dominance'],
            record['percentChange24h_vs_BTC'],
            record['percentChange7d_vs_BTC'],
            record['percentChange30d_vs_BTC'],
            record['percentChange60d_vs_BTC'],
            record['percentChange90d_vs_BTC']
        ) for record in data]
        
        pg_hook.insert_rows(
            table='crypto_historical',
            rows=rows,
            target_fields=[
                'symbol', 'cmcRank', 'marketPairCount', 'timestamp', 'volume24h',
                'volume7d', 'marketCap', 'vol24h_vs_MC', 'marketCapByTotalSupply',
                'fullyDilluttedMarketCap', 'percentChange1h', 'percentChange24h',
                'percentChange7d', 'dominance', 'percentChange24h_vs_BTC',
                'percentChange7d_vs_BTC', 'percentChange30d_vs_BTC',
                'percentChange60d_vs_BTC', 'percentChange90d_vs_BTC'
            ]
        )

    extract_data = extract_crypto_data()
    transformed_data = transform_crypto_data(extract_data)
    load_to_postgres(transformed_data)