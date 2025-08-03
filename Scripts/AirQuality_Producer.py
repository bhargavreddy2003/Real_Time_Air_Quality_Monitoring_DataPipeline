# producer_kafka_python.py
from kafka import KafkaProducer
import json
import time
import requests
import os

# Configuration for Azure Event Hubs
KAFKA_BOOTSTRAP_SERVERS = os.get("Kafka_server")
KAFKA_TOPIC =  os.get("weatherapistream")
SASL_USERNAME = "$ConnectionString"
SASL_PASSWORD = f"Endpoint=sb://{event_hub}.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey={os.get("Access_Key")}"
API_TOKEN=os.get("API-TOKEN")
# Cities
indian_cities = [
    'Agartala', 'Agra', 'Ahmedabad', 'Aizawl', 'Ajmer', 'Alwar', 'Amaravati', 'Amritsar', 'Anantapur', 'Ankleshwar',
    'Araria', 'Arrah', 'Asansol', 'Aurangabad', 'Baddi', 'Bagalkot', 'Baghpat', 'Bareilly', 'Bathinda', 'Belgaum',
    'Bengaluru', 'Bettiah', 'Bhilai', 'Bhiwadi', 'Bhopal', 'Bihar Sharif', 'Bilaspur', 'Brajrajnagar', 'Bulandshahr',
    'Chamarajanagar', 'Chandigarh', 'Chandrapur', 'Chennai', 'Chhapra', 'Chikkaballapur', 'Chikkamagaluru',
    'Coimbatore', 'Damoh', 'Davanagere', 'Dehradun', 'Delhi', 'Dewas', 'Dindigul', 'Eloor', 'Firozabad', 'Gadag',
    'Gandhinagar', 'Gangtok', 'Gaya', 'Ghaziabad', 'Gorakhpur', 'Greater Noida', 'Gummidipoondi', 'Guwahati', 'Gwalior',
    'Hajipur', 'Hapur', 'Hassan', 'Haveri', 'Hosur', 'Howrah', 'Hubballi', 'Hyderabad', 'Imphal', 'Indore', 'Jabalpur',
    'Jaipur', 'Jalandhar', 'Jhansi', 'Jodhpur', 'Jorapokhar', 'Kalaburagi', 'Kalyan', 'Kannur', 'Kanpur', 'Katihar',
    'Katni', 'Khanna', 'Khurja', 'Kishanganj', 'Kolkata', 'Kollam', 'Kota', 'Lucknow', 'Ludhiana', 'Madikeri', 'Maihar',
    'Mandideep', 'Mangalore', 'Manguraha', 'Meerut', 'Moradabad', 'Motihari', 'Mumbai', 'Munger', 'Muzaffarnagar',
    'Muzaffarpur', 'Mysuru', 'Nagpur', 'Naharlagun', 'Nandesari', 'Nashik', 'Ooty', 'Pali', 'Patiala', 'Patna',
    'Pithampur', 'Prayagraj', 'Puducherry', 'Purnia', 'Rajamahendravaram', 'Rajgir', 'Ramanagara', 'Ramanathapuram',
    'Ratlam', 'Rupnagar', 'Sagar', 'Saharsa', 'Samastipur', 'Sasaram', 'Shillong', 'Shivamogga', 'Siliguri',
    'Sivasagar', 'Siwan', 'Srinagar', 'Talcher', 'Thiruvananthapuram', 'Thoothukudi', 'Thrissur', 'Tirupur',
    'Udaipur', 'Vapi', 'Varanasi', 'Vatva', 'Vijayapura', 'Visakhapatnam', 'Vrindavan', 'Yadgir'
]

def create_kafka_producer():
    """Create and return a Kafka producer configured for Azure Event Hubs"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=SASL_USERNAME,
        sasl_plain_password=SASL_PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )


def send_json_data(producer, topic, data):
    """Send JSON data to Kafka topic"""
    try:
        future = producer.send(topic, value=data)
        # Wait for message to be delivered
        future.get(timeout=10)
        print(f"Successfully sent data to {topic}")
    except Exception as e:
        print(f"Failed to send message: {e}")


if __name__ == "__main__":
    producer = create_kafka_producer()
    while True:
        for city in indian_cities:
            try:
                url = f"http://api.waqi.info/feed/{city}/?token={API_TOKEN}"
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("status") == "ok":
                        data["city_name"] = city
                        send_json_data(producer, KAFKA_TOPIC, data)
                        print(f" Sent: {city}")
                    else:
                        print(f" Skipped (no data): {city}")
                else:
                    print(f" Error {resp.status_code} for {city}")
            except Exception as e:
                print(f" Exception for {city}: {e}")
            time.sleep(10)  # Avoid API rate limits

        print("\n Completed one full city cycle. Restarting...\n")





    # Close the producer
    producer.flush()
    producer.close()


    

