import pandas as pd
import paho.mqtt.client as mqtt
import pymysql

# Connect to the MQTT broker
broker_address = "localhost"
broker_port = 1883
client = mqtt.Client()
client.connect(broker_address, broker_port, 60)



#Publisher Code



            
def publish_data_to_mqtt(topic, data):
    # Publish data to the MQTT topic
    client.publish(topic, data)

def fetch_data_from_database():
    # Connect to the MySQL database
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='root',
        db='Nikhil',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        # Read data from the database
        with conn.cursor() as cursor:
            query = "SELECT * FROM d_expectation"  # Update with your table name
            cursor.execute(query)
            result = cursor.fetchone()
              # Convert data to MQTT message format (comma-separated key-value pairs)
            data_parts = [f"{key}:{value}" for key, value in result.items()]
        data = ','.join(data_parts)
    except (pymysql.Error, pymysql.Warning) as e:
        print(e)
        return None
    
      
    finally:
        # Close the database connection
        conn.close()

    return data

# Define the MQTT topic to which you want to publish the data
mqtt_topic = "your/mqtt/topic"  # Update with your desired topic

# Fetch data from the database and publish it to MQTT
data = fetch_data_from_database()
publish_data_to_mqtt(mqtt_topic, data)



# Connect to the MQTT broker
broker_address = "localhost"
broker_port = 1883
client.connect(broker_address, broker_port, 60)

#Calling the display database function python3 Publisher_code.py