import psycopg2




base_url = "https://randomuser.me/api/?nat=gb"
Parties = ["Conservative Party", "Labour Party", "Liberal Democrats",
           "Scottish National Party (SNP)", "Green Party"]

def create_tables(:conn,:cur):
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

    cur.except(
        """
        create table if not exists votes(
        voter_id varchar(255) unique,
        candi_id varchar(255),
        voting_time timestamp,
        vote in default 1,
        primary key(voter_id,candi_id)
        )
        """
    )

    conn.commit()    


if __name__ = "__main__":
    try:
        conn= psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()

        create_tables(conn,cur)

        cur.except("""
            select * from candidates
        """)
        candidates = cur.fetchall()
        print(candidates)

    except Exception as e:
        print(e)