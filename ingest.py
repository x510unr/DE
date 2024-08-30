

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time
import argparse, os, sys, logging, math, re
from datetime import date

def main(params):

    urls = ['https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet', 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet', 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv']
    db = params.db 
    pre = 1

    logging.info(f"Creating a connection to {db} db")

    engine = create_engine(f'postgresql://root:root@192.168.1.7:5432/{db}')

    logging.info("connection complete")
    
    for url in urls: 
        
        newfile = url.rsplit('/',1)[-1]

        file_name = re.sub(r'_[0-9-+]+','',newfile)

        table_name = file_name.split('.')[0]

        logging.info(f"Dowloading the file from {url}...")

        os.system(f"curl -SL {url} -o {file_name}")

        logging.info(f"Dowloading of {file_name} is complete")

        if '.parquet' in file_name:
            trips = pq.read_table(file_name).to_pandas()
        else: trips = pd.read_csv(file_name)


        #x= pd.io.sql.get_schema(trips, name='yellow_taxi_data', con=engine)

        logging.info(f"Creating {db}.{table_name} table in postgres")

        trips.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        logging.info(f"{table_name} table is created")

        end = 0; start = 0;

        step = lambda x : 100000 if x >= 100000 else x

        length = len(trips)

        chunk_size = step(length)

        chunks = math.ceil(length/chunk_size)
        
        logging.info(f"Starting the ingestion of data to {table_name} table")

        count = 1

        for start in range(0, length, chunk_size):
            t_start= time()
            end = start + chunk_size
            df = trips.iloc[start:end]
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            logging.info(f"Inserted {count}/{chunks} chunk in %.3f seconds" %(t_end - t_start))  ## approx 140 sec to insert the whole data.
            count = count + 1
            
        logging.info(f"Ingestion complete for {pre}/{len(urls)} - {table_name}")

        pre = pre + 1

if __name__=='__main__':

    parser = argparse.ArgumentParser(description="To load the data from parquet file link to postgres db")

    # parser.add_argument('--url', help='url for the .parquet file')
    parser.add_argument('--db', help='destination database name')
    # parser.add_argument('--table', help='destination table name')

    args = parser.parse_args()

    datee = date.today()

    logging.basicConfig(filename=f'/home/pipe/logs/ingest_py_{datee}.log', level = logging.DEBUG, format ='%(asctime)s - %(levelname)s - %(message)s')

    try:
        main(args)
    except Exception as err:
        print('An error occured: '+ str(err))
        logging.error('Error:'+str(err))