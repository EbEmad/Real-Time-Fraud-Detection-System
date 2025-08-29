import json
import os
import random
import time
from datetime import datetime

from dateutil.tz import gettz
from faker import Faker
from kafka import KafkaProducer
KAFKA_BOOTSTRAP_SERVERS=os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092")
KAFKA_TOPIC=os.getenv("KAFKA_TOPIC", "transactions")
RATE=float(os.getenv("PRODUCE_RATE_PER_SEC", "2"))

fake = Faker()

def generate_transaction()->dict:
    amount = round(random.lognormvariate(3, 1), 2)  # skewed positive amounts

    # add some injected fraudulent patterns
    if random.random()<0.05:
        amount*=random.uniform(5,20)

    return {
        "transaction_id": fake.uuid4(),
        "amount": float(amount),
        "features":{
            "num_items":int(max(1,random.gauss(2,1))),
            "merchant_risk": float(random.random()),
            "hour": float(datetime.now(tz=gettz("Africa/Cairo")).hour),
        },
        "event_time": datetime.utcnow().isoformat()+'Z',
    }

def main()->None:
    producer=None
    while producer is None:
        try:
            producer=KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                linger_ms=50,
            )
        except Exception:
            time.sleep(2)
    interval = 1.0 / RATE if RATE > 0 else 0.5
    while True:
        payload=generate_transaction()
        producer.send(KAFKA_TOPIC,value=payload)
        time.sleep(interval)
if __name__=="__main__":
    main()