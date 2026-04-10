import pandas as pd
import json
import time
import logging
from kafka import KafkaProducer
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093'
ORDERS_TOPIC = 'olist.orders'
ORDER_ITEMS_TOPIC = 'olist.order_items'
DATA_PATH = Path('data/raw/olist')
REPLAY_SPEED = 0.1  # saniye cinsinden event'ler arası bekleme

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )

def load_data():
    logger.info("Olist verileri yükleniyor...")
    orders = pd.read_csv(DATA_PATH / 'olist_orders_dataset.csv')
    order_items = pd.read_csv(DATA_PATH / 'olist_order_items_dataset.csv')
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    orders = orders.sort_values('order_purchase_timestamp')
    logger.info(f"Toplam {len(orders)} sipariş yüklendt.")
    return orders, order_items

def replay_orders(producer, orders, order_items):
    logger.info("Replay başlıyor...")
    for idx, order in orders.iterrows():
        order_dict = order.to_dict()
        order_dict['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()
        order_dict['source'] = 'olist_replay'

        producer.send(
            ORDERS_TOPIC,
            key=order_dict['order_id'],
            value=order_dict
        )

        items = order_items[order_items['order_id'] == order_dict['order_id']]
        for _, item in items.iterrows():
            item_dict = item.to_dict()
            item_dict['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()
            item_dict['source'] = 'olist_replay'
            producer.send(
                ORDER_ITEMS_TOPIC,
                key=item_dict['order_id'],
                value=item_dict
            )

        if idx % 100 == 0:
            producer.flush()
            logger.info(f"  {idx} sipariş gönderildi | order_id: {order_dict['order_id']}")

        time.sleep(REPLAY_SPEED)

    producer.flush()
    logger.info("Tüm siparişler Kafka'ya gönderildi.")

if __name__ == '__main__':
    producer = create_producer()
    orders, order_items = load_data()
    replay_orders(producer, orders, order_items)
    producer.close()
