from confluent_kafka import Producer
import random
import uuid
import datetime
import json
import time

#     kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic orders-5-min
#     kafka-console-consumer --bootstrap-server localhost:9092 --topic orders-5-min --from-beginning

TOPIC = "orders"
SAMPLES = 1000
DELAY = 10 # seconds

state_code = ("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL"	
         "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME"	
         "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH"	
         "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "PR"	
         "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "VI", "WA"	
         "WV", "WI", "WY")

producer = Producer({'bootstrap.servers': 'localhost:9092'})

for i in range(SAMPLES):
    order_id = random.randint(0, 10000)
    item_id = str(random.randint(0, 100))
       
    current_time = datetime.datetime.now()
 
    invoice_date = current_time.strftime('%m/%d/%Y %H:%M') 
    
    number_of_items = random.randint(1, 10)
    
    for j in range(number_of_items):
        qty = random.randint(1, 10)
        price = random.randint(1, 50)
        state = random.choice(state_code)
        invoice =   {  "OrderId":     order_id,
                       "ItemId":      item_id,
                       "Quantity":    qty,
                       "UnitPrice":   price,
                       "State":       state,                       
                       "InvoiceDate": invoice_date
                    }
    
        invoice_str = json.dumps(invoice)
        print ("POS ", invoice_str)

        key = invoice["State"]
        producer.produce(topic=TOPIC, value=invoice_str, key=key)
        
    time.sleep(DELAY)