from kafka import KafkaProducer
import json
import pandas as pd

producer = KafkaProducer(
   bootstrap_servers='localhost:9092',
   value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
def on_send_success(record_metadata):
   print(record_metadata.topic)
   print(record_metadata.partition)
   print(record_metadata.offset)

def on_send_error(excp):
   print('I am an errback', exc_info=excp)

data = json.load(open("results/data.json"))
MostSales = data.pop("MostSales")
TopSellers = data.pop("TopSellers")

df_most_sales = pd.DataFrame([MostSales])
df_top_sellers = pd.DataFrame(TopSellers)

for index, row in df_most_sales.iterrows():
    print(row["DateWithMostSales"], row["total_sales"])
    producer.send('mostsales', {row["DateWithMostSales"] : row["total_sales"]} ).add_callback(on_send_success).add_errback(on_send_error)


for index, row in df_top_sellers.iterrows():
    print(row["ToyID"], row["total_quantity"])
    producer.send('products', {"ToyId" : int(row["ToyID"]), "Unit" : int(row["total_quantity"])}).add_callback(on_send_success).add_errback(on_send_error)

producer.close()
print("Succesfully sent")
