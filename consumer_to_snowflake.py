import json
from kafka import KafkaConsumer
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
print('✅Library Imported Sucessfully.')

BOOTSTRAP_SERVERS="localhost:29092"
TOPIC_NAME="clean_events"
GROUP_ID = "snowflake-loader"

snowflake_config= {
    "user" : 'LAVI',
    'password' : 'Snowflake12345',
    'account' : 'ps83865.ap-southeast-1',
    'warehouse' : 'COMPUTE_WH',
    'database' : 'KAFKA_DB',
    'schema' : 'STREAMING'
}

BATCH_SIZE = 10

consumer= KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers  = BOOTSTRAP_SERVERS,
    group_id = GROUP_ID,
    enable_auto_commit=False,
    auto_offset_reset = 'earliest',
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")))

sf_connector = snowflake.connector.connect(**snowflake_config)
print('Connected to Snowflake --->>>')
print('Starting Kafka -->> Snowflake Loader')

buffer=[]

def flush_to_snowflake(records):
    df=pd.DataFrame(records)
    df.columns = [c.upper() for c in df.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn= sf_connector,
        df=df,
        table_name='KAFKA_EVENTS_SILVER'
    )

    if not success:
        raise Exception('Snowflake Insert Failed')
    print(f'Inserted {nrows} rows into snowflake.')

for message in consumer:
    event = message.value
    buffer.append({
        "event_id" : event['event_id'],
        "customer_id" : event['customer_id'],
        'event_type' : event['event_type'],
        'amount' : event['amount'],
        'currency' : event['currency'],
        'event_timestamp' : event['event_timestamp']
    })

    if len(buffer) >= BATCH_SIZE:
        try:
            flush_to_snowflake(buffer)
            consumer.commit()
            buffer.clear()
        except Exception as e:
            print(f'Error Inserting Batch -->> {e}')















