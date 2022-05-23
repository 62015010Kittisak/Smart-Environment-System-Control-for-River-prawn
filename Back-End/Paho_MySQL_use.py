import paho.mqtt.client as mqtt
import mysql.connector as mysql
import datetime


db = mysql.connect(
    host = "localhost",
    user = "root",
    passwd = "!Try620150",
    database = "Environment_System_Control_for_River_prawn"
)

cursor = db.cursor()



def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected with result code {0}".format(str(rc)))  # Print result of connection attempt
    client.subscribe("sensor_data/recive_to_databases")  # Subscribe to the topic “digitest/test1”, receive any messages published on it


def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the serve
    txt = str(msg.payload)
    msg_payload = txt.split("'")
    ##text to list
    values = msg_payload[1].split(",")
    print(values)  # Print a received msg
    print(len(values))
    if(len(values) == 8):
	    #print("Result = " + str(values))
	    ## executing the query with values
	    try:
		    insert = "INSERT INTO tb_sensor (Mac_sensor, Temperature_w, Ph, turbidity, Temperatur_a, Humidity_a, Light, EC_Sensor) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
		    cursor.execute(insert, values)    
		    ## to make final output we have to run the 'commit()' method of the database object
		    db.commit()
		    print(cursor.rowcount, "record inserted")
	    except Exception:
	    	print("!!!!!! ERROR But i'm setting to Ignore it. !!!!!!!!")
	    	pass



client = mqtt.Client("save_mqtt_database")  # Create instance of client with client ID “digi_mqtt_test”
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
# client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
client.username_pw_set("mymqtt", "myraspi")
client.connect('127.0.0.1', 1883)
client.loop_forever()  # Start networking daemon
