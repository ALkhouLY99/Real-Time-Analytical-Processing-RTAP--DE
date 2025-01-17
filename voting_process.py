import os
import psycopg2
import logging
import json
import random 
from datetime import datetime
from dotenv import load_dotenv
from main import delivery_report
from confluent_kafka import Consumer,KafkaError,KafkaException,SerializingProducer

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load environment variables from .env file (if using python-dotenv)
load_dotenv()

## setup kafka consumer
confg = {'bootstrap.servers': 'localhost:9092'}
consumer = Consumer(
        {
            **confg,
            'group.id': 'voting-group',       ## group_consum
             'enable.auto.commit': False,         # exactly-once processing 
             'auto.offset.reset': 'earliest'    ## read from begining
        })

## setup kafka producer
producer = SerializingProducer(
    { **confg,
    'key.serializer': lambda k, ctx: str(k).encode('utf-8'),
    'value.serializer': lambda v, ctx: json.dumps(v).encode('utf-8'),
     'enable.idempotence': True                                            # Enable idempotence
    })

if __name__ == "__main__":

    try:
        conn=psycopg2.connect(host= os.getenv("DB_HOST"),dbname= os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
        )
        cur = conn.cursor()

        ## reterive data from psql
        cur.execute("""
                    select row_to_json(col)
                    from 
                    (SELECT * FROM candidates) col
                    """)
        candi= [candidate[0] for candidate in cur.fetchall()]
        if not candi:
            raise Exception ("No candidates found in db")
        else:
            logging.info(candi)

        ## consume_message
        consumer.subscribe(['voters_topic'])
        try:
            while True :
                msg= consumer.poll(timeout=1)
                if msg is None:
                    continue
                elif msg.error():
                    if msg.error().code()== KafkaError._PARTITION_EOF:
                         continue
                    else:
                        logging.error(msg.error())
                else:
                    voter = json.loads(msg.value().decode('utf-8'))  ## comein in from kafka_consumer

                    chose_candi= random.choice(candi)   ## comein in from psql

                    voting = {
                        **voter,
                        **chose_candi,
                        'voting_time': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        'vote': 1
                    }

                    ## insert data to psql
                    try:                   
                        cur.execute(
                            """
                            insert into votes(voter_id,candi_id,voting_time)
                            values(%s,%s,%s)
                            ON CONFLICT (voter_id) DO NOTHING
                            """,
                                (voting['voter_id'],voting['candi_id'],voting['voting_time'])            
                            )
                            
                        conn.commit()

                        ## produce_voting_data
                        producer.produce(
                            topic= 'votes_topic',
                            key = voting['voter_id'],
                            value = voting,
                            on_delivery = delivery_report
                            )
                            # Call poll to handle delivery reports
                        producer.poll(0)          # Non-blocking call to process delivery reports
                        print(f'Produced voter , data: {voting}\n')
                    except Exception as e:
                        logging.error(f"An error occurred while processing the vote: {e}")
                        conn.rollback()
        except KeyboardInterrupt:
            logging.info("Consumer interrupted. Shutting down...")
        finally:
            # Close the consumer and producer
            consumer.close()
            producer.flush()
            logging.info("Kafka consumer and producer closed")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        # Close the database connection
        if conn:
            cur.close()
            conn.close()
            logging.info("Database connection closed")
        