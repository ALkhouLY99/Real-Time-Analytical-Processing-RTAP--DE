import psycopg2
import requests
import json
from confluent_kafka import SerializingProducer
import logging
import time
import itertools

# Configure logging
logging.basicConfig(level=logging.INFO)

base_url = "https://randomuser.me/api/?nat=gb"
Parties = ["Conservative Party", "Labour Party", "Liberal Democrats",
           "Scottish National Party (SNP)", "Green Party"]

def candidates_info():
    candidates = [
        {
            "candi_id": "C01",  # Predefined constant UUID
            "candi_name": "Rishi Sunak",
            "party_affiliation": "Conservative Party",
            "Bio": "Rishi Sunak is the Prime Minister of the United Kingdom and leader of the Conservative Party.",
            "campaign_platform": "Focusing on economic growth, reducing taxes, and maintaining the Union.",
            "photo_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/3/30/Official_Portrait_of_Prime_Minister_Rishi_Sunak_%28crop_with_flag%29.jpg/384px-Official_Portrait_of_Prime_Minister_Rishi_Sunak_%28crop_with_flag%29.jpg"
        },
        {
            "candi_id": "C02",  # Predefined constant UUID
            "candi_name": "Keir Starmer",
            "party_affiliation": "Labour Party",
            "Bio": "Keir Starmer is the leader of the Labour Party, focusing on social equality and worker's rights.",
            "campaign_platform": "Investing in public services, reducing inequality, and combating climate change.",
            "photo_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/c/c4/Prime_Minister_Sir_Keir_Starmer_Official_Portrait.jpg/640px-Prime_Minister_Sir_Keir_Starmer_Official_Portrait.jpg"
        },
        {
            "candi_id": "C03",  # Predefined constant UUID
            "candi_name": "Ed Davey",
            "party_affiliation": "Liberal Democrats",
            "Bio": "Ed Davey is the leader of the Liberal Democrats, advocating for individual rights and environmental policies.",
            "campaign_platform": "Fighting for a green economy, mental health reforms, and electoral reform.",
            "photo_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Ed_Davey_Bournemouth_2017_%28cropped%29.jpg/532px-Ed_Davey_Bournemouth_2017_%28cropped%29.jpg"
        },
         {
            "candi_id": "C04", # Predefined constant UUID
            "candi_name": "Nicola Sturgeon",
            "party_affiliation": "Scottish National Party (SNP)",
            "Bio": "Nicola Sturgeon is the former First Minister of Scotland and a leader in the fight for Scottish independence.",
            "campaign_platform": "Achieving Scottish independence and promoting social justice and equality.",
            "photo_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/3/34/First_Minister%2C_Nicola_Sturgeon.jpg/447px-First_Minister%2C_Nicola_Sturgeon.jpg"
        },
         {
            "candi_id": "C05", # Predefined constant UUID
            "candi_name": "Jonathan Bartley",
            "party_affiliation": "Green Party of England and Wales",
            "Bio": "Jonathan Bartley is a co-leader of the Green Party, pushing for environmental sustainability and social equality.",
            "campaign_platform": "Fighting climate change, promoting renewable energy, and social justice.",
            "photo_url": "https://upload.wikimedia.org/wikipedia/commons/thumb/6/6a/Green_Party_Autumn_Conference_2016_01.jpg/640px-Green_Party_Autumn_Conference_2016_01.jpg"
        }
    ]
    return candidates


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
        ON CONFLICT (voter_id) DO NOTHING
        """, (
            voter_data['voter_id'], voter_data['voter_name'],
            voter_data['dob'], voter_data['gender'],
            voter_data['nationality'], voter_data['registration_number'],
            json.dumps(voter_data['address']), voter_data['email'],        ## json.dumps()
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
    'value.serializer': lambda v, ctx: json.dumps(v).encode('utf-8'),
     'enable.idempotence': True                     # Enable idempotence
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
            candi_list = candidates_info()
            for i,val in enumerate(candi_list):
                print(f"inserting candidate {i+1}:--> {val}\n")

                if val:
                    cur.execute(
                    """
                    INSERT INTO candidates (candi_id, candi_name, party_affiliation, Bio, campaign_platform, photo_url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (val['candi_id'], val['candi_name'], val['party_affiliation'],
                     val['Bio'], val['campaign_platform'], val['photo_url'])
                )
                conn.commit()



        # if not candidates:
        #     for i in range(len(Parties)):
        #         candi = generate_candi(i, len(Parties))
        #         cur.execute(
        #             """
        #             INSERT INTO candidates (candi_id, candi_name, party_affiliation, Bio, campaign_platform, photo_url)
        #             VALUES (%s, %s, %s, %s, %s, %s)
        #             """,
        #             (candi['candi_id'], candi['candi_name'], candi['party_affiliation'],
        #              candi['Bio'], candi['campaign_platform'], candi['photo_url'])
        #         )
        #         conn.commit()

        # pull data from source to insert into db & produce_to_kafka
        # for i in range(1000000):
        for i in itertools.count():                   # infinite loops
            voter_data = generate_voter_d()
            insert_voters_data(conn, cur, voter_data)

            # produce data to kafka  
            producer.produce(
                topic=voters_topic,
                key=voter_data['voter_id'],
                value=voter_data,
                on_delivery=delivery_report
            )

            producer.poll(0)    # Call poll() to process delivery reports
            print(f'Produced voter {i}: ---> data: {voter_data}\n')
            time.sleep(.3)

    # Wait for all messages to be delivered
        producer.flush()

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        conn.rollback()
    finally:
        # flush is called in the finally block
        producer.flush()
        # Close the cursor and connection
        cur.close()
        conn.close()