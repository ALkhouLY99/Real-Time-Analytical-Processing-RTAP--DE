import psycopg2
import requests
import json
from confluent_kafka import SerializingProducer

base_url = "https://randomuser.me/api/?nat=gb"
Parties = ["Conservative Party", "Labour Party", "Liberal Democrats",
           "Scottish National Party (SNP)", "Green Party"]

def create_tables(conn,cur):
    cur.execute(
        """
        create table if not exists candidates(
        candi_id varchar(255) primary key,
        candi_name varchar(255),
        party_affiliation varchar(255),
        Bio text,
        campaign_platform text,
        photo_url text    
        
        )   
        """
    )

    cur.execute(
        """
        create table if not exists voters(
         voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            dob VARCHAR(255),
            gender VARCHAR(20),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address jsonb,
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age int
        )  
        """
    )

    cur.execute(
        """
        create table if not exists votes(
        voter_id varchar(255) unique,
        candi_id varchar(255),
        voting_time timestamp,
        vote int  default 1,
        primary key(voter_id,candi_id)
        )
        """
    )

    conn.commit() 

## denerate_candidate_date
def generate_candi(num_candi,total_parties):                                
    res= requests.get(base_url + '&gender='+ ('female' if num_candi % 2 == 0 else 'male'),'none')
    
    if res.status_code == 200:
        user_data = res.json()['results'][0]
        
        return {
            'candi_id':user_data['login']['uuid'],
            'candi_name': f"{user_data['name']['first']} {user_data['name']['last']}",
            'party_affiliation': Parties[num_candi % total_parties],
            'Bio':'hey gays whats up ',
            'campaign_platform':'anyfuckinthings',
            'photo_url':user_data['picture']['large']

        }
    else:
        raise Exception(f"Failed to fetch data: {res.status_code}")

## generate_voters_Data
def generate_voter_d():                                    
    res= requests.get(base_url)
    if res.status_code ==200:
        voter_data = res.json()['results'][0]

        return {
                'voter_id': voter_data['login']['uuid'],
                'voter_name' : f"{voter_data['name']['first']} {voter_data['name']['last']}",
                'dob':voter_data['dob']['date'][:10],
                'gender': voter_data['gender'],
            'nationality' : voter_data['nat'],
            'registration_number':voter_data['login']['username'],
            'address':{
                'street': voter_data['location']['street']['name'],
                'city': voter_data['location']['city'],
                'state':voter_data['location']['state'],
                'country':voter_data['location']['country'],
                'postcode':voter_data['location']['postcode']
            },
            'email':voter_data['email'],
            'phone_number':voter_data['phone'],
            'cell_number':voter_data['cell'],
            'picture':voter_data['picture']['large'],
            'registered_age':voter_data['registered']['age']
        }
    else:
        raise Exception(f"Failed to fetch data: {res.status_code}") 






def insert_voters_data(conn,curs,voter_data):
    curs.execute(
        """
        INSERT INTO voters (voter_id, voter_name, dob, gender, nationality,registration_number,address, email, phone_number, cell_number, picture, registered_age)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """ ,(voter_data['voter_id'],voter_data['voter_name'],
                voter_data['dob'],voter_data['gender'],
                voter_data['nationality'],voter_data['registration_number']
                ,json.dumps(voter_data['address']),voter_data['email'],           ## Serialize dictionary to JSON
                voter_data['phone_number'],voter_data['cell_number'],
                voter_data['picture'],voter_data['registered_age'])
                )
    conn.commit()



if __name__ == "__main__":
    producer = SerializingProducer
    try:
        conn= psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()

        create_tables(conn,cur)                           ## create tables_in db_postgre

        cur.execute("""
            select * from candidates
        """)
        candidates = cur.fetchall()
        print(candidates)

        if not candidates :
            for i in range(len(Parties)):
                candi = generate_candi(i,len(Parties))
                cur.execute(
                    """
                    insert into candidates(candi_id, candi_name, party_affiliation, Bio,campaign_platform, photo_url)
                    values(%s,%s,%s,%s,%s,%s)
                """,
                    (candi['candi_id'],
                    candi['candi_name'],candi['party_affiliation'],
                    candi['Bio'],candi['campaign_platform'],
                    candi['photo_url']))
                
                conn.commit()


        for i in range(1000):
            voter_data = generate_voter_d()
            print(voter_data)
            insert_voters_data(conn,cur,voter_data)
            break


    except Exception as e:
        print(f"An error occurred:{e}")
        conn.rollback()
    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()