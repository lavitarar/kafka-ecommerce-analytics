import json 
from kafka import KafkaProducer , KafkaConsumer
print('✅Library Imported Sucessfully')

BOOT_STRAP_SERVER="localhost:29092"
INPUT_TOPIC='customer_raw_events'
OUTPUT_TOPIC='clean_events'
GROUP_ID='silver-stream-processor'

VALID_EVENT_TYPE= ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

consumer=KafkaConsumer(
    INPUT_TOPIC,
    bootstrap_servers=BOOT_STRAP_SERVER,
    group_id = GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")))


producer=KafkaProducer(
    bootstrap_servers=BOOT_STRAP_SERVER,
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))


def is_valid_event(event):
    if not event.get('customer_id'):
        return False
    if event.get('event_type') not in VALID_EVENT_TYPE:
        return False
    if event.get('amount') is None or event.get('amount') <=0:
        return False
    if not event.get('currency'):
        return False
    if event.get('is_valid') is not True:
        return False
    return True

print('✅Starting Silver Stream Processor ---->>>>>>>>')
Total_valid =0
Total_invalid = 0

for message in consumer:
    key = message.key
    event=message.value

    if is_valid_event(event):
        producer.send(
            topic=OUTPUT_TOPIC,
            key=key,
            value=event )
        print(f'FORWARDED | key = {key} |event_type = {event['event_type']}')
        Total_valid += 1
        print(f'Total Valid -->> {Total_valid}')
        print(f'Total Valid -->> {Total_invalid}')
    
    else:
        print(f'DROPPED | key ={key} | reason = invalid ')
    
    consumer.commit()
    print('Data Saved Sucessfully')
