import psycopg2
import requests
import json
from confluent_kafka import SerializingProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

base_url = "https://randomuser.me/api/?nat=gb"
Parties = ["Conservative Party", "Labour Party", "Liberal Democrats",
           "Scottish National Party (SNP)", "Green Party"]

def create_tables(conn, cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
            candi_id VARCHAR(255) PRIMARY KEY,
            candi_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            Bio TEXT,
            campaign_platform TEXT,
            photo_url TEXT    
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            dob VARCHAR(255),
            gender VARCHAR(20),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address JSONB,
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candi_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote INT DEFAULT 1,
            PRIMARY KEY (voter_id, candi_id)
        )
        """
    )

    conn.commit()

def generate_candi(num_candi, total_parties):
    res = requests.get(base_url + '&gender=' + ('female' if num_candi % 2 == 0 else 'male'), 'none')
    
    if res.status_code == 200:
        user_data = res.json()['results'][0]
        
        return {
            'candi_id': user_data['login']['uuid'],
            'candi_name': f"{user_data['name']['first']} {user_data['name']['last']}",
            'party_affiliation': Parties[num_candi % total_parties],
            'Bio': 'Hey guys, what\'s up?',
            'campaign_platform': 'Any platform',
            'photo_url': user_data['picture']['large']
        }
    else:
        raise Exception(f"Failed to fetch data: {res.status_code}")

def generate_voter_d():
    res = requests.get(base_url)
    if res.status_code == 200:
        voter_data = res.json()['results'][0]

        return {
            'voter_id': voter_data['login']['uuid'],
            'voter_name': f"{voter_data['name']['first']} {voter_data['name']['last']}",
            'dob': voter_data['dob']['date'][:10],
            'gender': voter_data['gender'],
            'nationality': voter_data['nat'],
            'registration_number': voter_data['login']['username'],
            'address': {
                'street': voter_data['location']['street']['name'],
                'city': voter_data['location']['city'],
                'state': voter_data['location']['state'],
                'country': voter_data['location']['country'],
                'postcode': voter_data['location']['postcode']
            },
            'email': voter_data['email'],
            'phone_number': voter_data['phone'],
            'cell_number': voter_data['cell'],
            'picture': voter_data['picture']['large'],
            'registered_age': voter_data['registered']['age']
        }
    else:
        raise Exception(f"Failed to fetch data: {res.status_code}")

def insert_voters_data(conn, curs, voter_data):
    curs.execute(
        """
        INSERT INTO voters (voter_id, voter_name, dob, gender, nationality, registration_number, address, email, phone_number, cell_number, picture, registered_age)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            voter_data['voter_id'], voter_data['voter_name'],
            voter_data['dob'], voter_data['gender'],
            voter_data['nationality'], voter_data['registration_number'],
            json.dumps(voter_data['address']), voter_data['email'],
            voter_data['phone_number'], voter_data['cell_number'],
            voter_data['picture'], voter_data['registered_age']
        )
    )
    conn.commit()

def delivery_report(err, msg):
    """Callback for delivery report. Called once for each message produced."""
    if err is not None:
        logging.error(f"Delivery failed for record {msg.key()}: {err}")
    else:
        logging.info(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Kafka topic
voters_topic = "voters_topic"

# Kafka producer configuration
producer = SerializingProducer({
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': lambda k, ctx: str(k).encode('utf-8'),
    'value.serializer': lambda v, ctx: json.dumps(v).encode('utf-8')
})

# main:-Entry_point
if __name__ == "__main__":
   

    try:
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()

        # Create tables in the database
        create_tables(conn, cur)  
        
        cur.execute("SELECT * FROM candidates")
        candidates = cur.fetchall()
        print(candidates)

        if not candidates:
            for i in range(len(Parties)):
                candi = generate_candi(i, len(Parties))
                cur.execute(
                    """
                    INSERT INTO candidates (candi_id, candi_name, party_affiliation, Bio, campaign_platform, photo_url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (candi['candi_id'], candi['candi_name'], candi['party_affiliation'],
                     candi['Bio'], candi['campaign_platform'], candi['photo_url'])
                )
                conn.commit()

        # pull data from source to insert into db & produce_to_kafka
        for i in range(1000):
            voter_data = generate_voter_d()
            insert_voters_data(conn, cur, voter_data)

            # produce data to kafka  
            producer.produce(
                topic=voters_topic,
                key=voter_data['voter_id'],
                value=voter_data,
                on_delivery=delivery_report
            )

            print(f'Produced voter {i}, data: {voter_data}\n')

    # Wait for all messages to be delivered
        producer.flush()

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        conn.rollback()
    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()