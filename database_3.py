import pandas as pd
import sqlalchemy
import pymysql
import paho.mqtt.client as mqtt



class dataBase : 
        

        
        def __init__(self,**kwargs):
            # Initialize the attributes based on the given keyword arguments
            for key, value in kwargs.items():
                    setattr(self, key, value)
        @classmethod
        def from_mqtt_message(cls,mqtt_message):
            # Parse the MQTT message and extract the necessary data to create the instance
            # Modify this method to match the format of the MQTT message you receive
            data = mqtt_message.payload.decode()
            data_parts = data.split(',')  # Assuming the data is comma-separated
             # Assuming the data_parts represent key-value pairs (e.g., "key1:value1,key2:value2")
            data_dict = dict(part.split(':') for part in data_parts)
            return cls(**data_dict)  # Pass the data as keyword arguments
        
#establishing the connection
engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost:3306/Nikhil')

#reading the table
table_name="d_expectation"
df =pd.read_sql_table(table_name,engine)
print(df)


#connecting to broker and inserting data into the database
def on_connect(client, userdata, flags, rc):
            print("Connected with result code " + str(rc))
            # Subscribe to the MQTT topic from which you receive the data
            client.subscribe("your/mqtt/topic")
def on_message(client, userdata, message):
            # Handle the incoming MQTT message here and create/update the AutoDiscoveries instance
            discovery = dataBase.from_mqtt_message(message)
            # Now you can use 'discovery' instance with the received data
            '''print("Received data for id:", discovery.id)'''
            #push the data into database
            insert_data_into_database(discovery)

def insert_data_into_database(data):
    # Connect to the MySQL database
   

    # Convert the data into a DataFrame
    data_dict = {k: [v] for k, v in data.__dict__.items()}
    df = pd.DataFrame(data_dict)
    for lols in data_dict:
          print(lols)
    try:
        # Insert the DataFrame into the database
        conn = pymysql.connect(
        host='localhost',
        user='root',  # Replace with your MySQL username
        password='root',  # Replace with your MySQL password
        db='Nikhil',  # Replace with your database name
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
        with conn.cursor() as cursor:
            table_name = 'new_haha'  # Replace with your desired table name
            columns = ', '.join(df.columns)
            values = ', '.join(['%s'] * len(df.columns))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cursor.executemany(sql, df.values.tolist())
        conn.commit()
    except (pymysql.Error, pymysql.Warning) as e:
        print(e)
        return None
    finally:
        
        conn.close()


# Configure the MQTT client and callbacks
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
# Connect to the MQTT broker
broker_address = "localhost"
broker_port = 1883
client.connect(broker_address, broker_port, 60)
# Start the MQTT client's network loop
client.loop_forever()


