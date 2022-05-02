from flask import Flask, request
import json
import csv
import psycopg2
import pandas as pd
app = Flask(__name__)

connection = psycopg2.connect(
    host = "localhost",
    database = "postgres",
    user = "postgres",
    password = "Charger.123"
)
cur = connection.cursor()
def connect(connection):
    """ Connect to the PostgreSQL database server """
 
    conn = connection
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**connection)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    print("Connection successful")
    return conn
def postgresql_to_dataframe(conn, select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1
    
    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()
    
    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    return df
conn = connect(connection)
column_name = ["Date/Time", "MsgNr", "Event" , "Message Class" , "Message Type" , "MessageStatus"]
df = postgresql_to_dataframe(conn, '''select * from Public."Alarm"''', column_name)
#print(df.head())
# print(df.to_numpy())
df[['Equipment_no', 'Alarm type']] = df['Event'].str.split('_', 1, expand=True)
count = df['Equipment_no'].value_counts()
count2 = df['Alarm type'].value_counts()
# count = count.to_dict()
# count2 = count2.to_dict()
# array = []
# reuslt={}
# items = {}
# for i in count:
#     items= {}
#     items["tag"] = i
#     items["id"] = count[i]
#     array.append(items)
count = count.to_dict()
@app.route("/", methods = ['POST']) 
def print2():
    return json.dumps({"result":count})

connection.commit()
cur.close()
connection.close()



if __name__ == "__main__": 
    app.run(port=5000)