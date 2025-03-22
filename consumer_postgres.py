from kafka import KafkaConsumer
import json
import psycopg2
print('connecting pg ...')
try:
    conn = psycopg2.connect(database = "defaultdb", 
                        user = "avnadmin", 
                        host= 'retobigdata-retobigdata.j.aivencloud.com',
                        # password = "",
                        port = 26035)
    cur = conn.cursor()
    print("PosgreSql Connected successfully!")
except:
    print("Could not connect to PosgreSql")

consumer = KafkaConsumer('products',bootstrap_servers=['localhost:9092'])
# Parse received data from Kafka
for msg in consumer:
    record = json.loads(msg.value)
    ToyID = record['ToyId']
    Units = record['Unit']

    try:
       sql = f"INSERT INTO products(ToyId, Units) VALUES({ToyID},{Units})"
       print(sql)
       cur.execute(sql)
       conn.commit()
    except:
       print("Could not insert into PostgreSql")
conn.close()
