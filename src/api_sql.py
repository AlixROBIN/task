from sqlalchemy import create_engine, text
from faker import Faker
import random
from datetime import datetime
from flask import Flask

db_string = "postgresql://root:root@localhost:5432/postgres"

engine = create_engine(db_string)

app = Flask(__name__)

@app.route("/users",methods=['GET'])
def get_users():
    users = run_sql_with_result( """
    SELECT * from users 
    """)
    data = []
    for row in users :
        user ={
            "id" : row[0],
            "firstname" : row[1],
            "lastname" : row[2],
            "age" : row[3],
            "email" : row[4],
            "job" : row[5]
        }
    
        data.append(user)
    return data


create_user_table_sql = """
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    firstname VARCHAR(100),
    lastname VARCHAR(100),
    age INT,
    email VARCHAR(200),
    job VARCHAR(100)
)
"""

create_application_table_sql = """
CREATE TABLE IF NOT EXISTS  application (
    id SERIAL PRIMARY KEY,
    appname VARCHAR(100),
    username VARCHAR(100),
    lastconnection TIMESTAMP WITH TIME ZONE,
    users_id INTEGER REFERENCES users(id)
)
"""

def run_sql_with_result(query: str):
    with engine.connect() as connection:
        trans = connection.begin()  
        result = connection.execute(text(query))
        trans.commit()  
        return result


fake = Faker()

def populate_tables():
    app=['facebook', 'instagram', 'whatsapp', 'snapchat', 'tiktok', 'twitter']
    for i in range(99):
        firstname = fake.first_name()
        lastname = fake.last_name()
        age = random.randrange(18,50)
        email =fake.email()
        job = fake.job().replace("'","")
        users_query = f"""
            INSERT INTO users (firstname, lastname, age,email,job) 
            VALUES ('{firstname}', '{lastname}', '{age}', '{email}','{job}')
            RETURNING id
        """
        users_id = run_sql_with_result(users_query).scalar()
        num_app = random.randint(0,5)
        for i in range(num_app) :
            appname =  random.choice(app)
        username = fake.user_name()
        lastconnection = datetime.now()
        application_query = f"""
            INSERT INTO application (appname, username, lastconnection, users_id)
            VALUES ('{appname}','{username}','{lastconnection}','{users_id}')
        """
        run_sql_with_result(application_query)




if __name__ == '__main__':
    run_sql_with_result(create_user_table_sql)
    run_sql_with_result(create_application_table_sql)
    populate_tables()
    app.run(host="0.0.0.0", port=8081, debug = True)
    



    