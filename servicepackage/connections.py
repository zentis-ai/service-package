from pymongo import MongoClient
import os
import pika
from dotenv import load_dotenv
import logging
logger = logging.getLogger(__name__)
load_dotenv()

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DATABASE = os.getenv('MONGO_DATABASE', 'dev')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_USERNAME = os.getenv('RABBITMQ_USERNAME')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')

def get_mongo_client():
    """
    Returns a MongoDB client connected to the local MongoDB instance.
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DATABASE]
        return client, db
    except Exception as e:
        logging.exception(f"Error connecting to MongoDB: {e}")
        return None
    
def get_pika_connection():
    """
    Returns a RabbitMQ connection using pika.
    """
    logger.info(f'Connecting to RabbitMQ at {RABBITMQ_HOST} with user {RABBITMQ_USERNAME}, password {RABBITMQ_PASSWORD}')
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    return pika.BlockingConnection(
        pika.ConnectionParameters(RABBITMQ_HOST, 
                                  5672,
                                  '/',
                                  credentials=credentials,
                                  socket_timeout=5,
                                  heartbeat=0))
    
    
if __name__ == "__main__":
    _, db = get_mongo_client()
    collection = db['agents']
    documents = collection.find()
    for doc in documents:
        logger.info(doc)