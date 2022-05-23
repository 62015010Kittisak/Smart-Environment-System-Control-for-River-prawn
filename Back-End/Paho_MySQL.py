import paho.mqtt.client as mqtt
import mysql.connector as mysql
import datetime


db = mysql.connect(
    host = "localhost",
    user = "Tony24Hours",
    passwd = "!Tony2557",
    database = "Energy_Report_Database"
)

cursor = db.cursor()



def on_connect(client, userdata, flags, rc):  # The callback for when the client connects to the broker
    print("Connected with result code {0}".format(str(rc)))  # Print result of connection attempt
    client.subscribe("mynew/test")  # Subscribe to the topic “digitest/test1”, receive any messages published on it


def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the serve
    txt = str(msg.payload)
    x = txt.split("'")
    y = x[1].split(",")
    print(y)  # Print a received msg
    x = datetime.datetime.now()
    date = x.strftime("%Y-%m-%d")
    time = x.strftime("%H:%M:%S")
    TS = x.strftime("%H:00:00")
    TE = x.strftime("%H:59:59")
    ##print("Time Start = " + TS + " Time End = " + TE)
    values = (y[0], y[1], y[2], y[3], str(date), str(time))
    ## query for update
    query = """SELECT ID_Device ,Sensor1 ,Sensor2 ,Sensor3 FROM Log WHERE ID_Device Like %s AND Time BETWEEN %s AND %s AND Date BETWEEN %s AND %s"""
    cursor.execute(query,(y[0],TS,TE,date,date,))
    records = cursor.fetchall()
    db.commit()
    
    
    if len(records) == 0:
    	print("Result = " + str(records))
    	## executing the query with values
    	insert = "INSERT INTO Log (ID_Device, Sensor1, Sensor2, Sensor3, Date, Time) VALUES (%s, %s, %s, %s, %s, %s)"
    	cursor.execute(insert, values)    
    	## to make final output we have to run the 'commit()' method of the database object
    	db.commit()
    	print(cursor.rowcount, "record inserted")
    	
    	
    else:
    	print("Result = " + str(records[0][1]) + "-" + str(records[0][2]) + "-" + str(records[0][3])) 
    	temp1 = float(records[0][1]) + float(y[1])
    	temp2 = float(records[0][2]) + float(y[2])
    	temp3 = float(records[0][3]) + float(y[3])
    	print("Result = " + str(temp1) + "-" + str(temp2) + "-" + str(temp3))
    	##Update
    	sql = """UPDATE Log SET Sensor1 = %s ,Sensor2 = %s ,Sensor3 = %s ,Date = %s ,Time = %s WHERE ID_Device Like %s AND Time BETWEEN %s AND %s AND Date BETWEEN %s AND %s"""
    	val = (str(temp1),str(temp2),str(temp3),date,time, y[0],TS,TE,date,date)
    	cursor.execute(sql, val)
    	db.commit()
    	print(cursor.rowcount, "record(s) affected")
    	print("Update")


client = mqtt.Client("digi_mqtt_test")  # Create instance of client with client ID “digi_mqtt_test”
client.on_connect = on_connect  # Define callback function for successful connection
client.on_message = on_message  # Define callback function for receipt of a message
# client.connect("m2m.eclipse.org", 1883, 60)  # Connect to (broker, port, keepalive-time)
client.username_pw_set("mymqtt", "myraspi")
client.connect('127.0.0.1', 1883)
client.loop_forever()  # Start networking daemon
