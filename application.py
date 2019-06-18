import os
import random
import time

import pyodbc
import pandas as pd
import redis as redis
from flask import Flask, render_template, request
import sqlite3 as sql

from math import radians, sin, cos, sqrt, atan2
myHostname = "azureassignment3.redis.cache.windows.net"
myPassword = "xw5S6heXPfqGZL4PfzatH+d7nnCawcY5dSMNTyWC+qQ="
server = 'mysqlserversuchitra.database.windows.net'
database = 'assignment3'
username = 'azureuser'
password = 'Geetha1963@'
driver= '{ODBC Driver 17 for SQL Server}'

r = redis.StrictRedis(host=myHostname, port=6380,password=myPassword,ssl=True)

app = Flask(__name__)
port = int(os.getenv('VCAP_APP_PORT','5000'))
#port = os.getenv('VCAP_APP_PORT','5000')

@app.route('/')
def home():
    return render_template('home.html')


@app.route('/list')
def list():
    con = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = con.cursor()
    cursor.execute("SELECT * FROM all_month")
    row = cursor.fetchall()
    return render_template("list.html", data1=row)


@app.route('/records')
def records():
    return render_template('records.html')


@app.route('/restricted')
def restricted():
    return render_template('rest.html')

@app.route('/restrictedmag')
def restrictedmag():
    return render_template('restmag.html')

@app.route('/dist')
def dist():
    return render_template('location.html')


@app.route('/time')
def time1():
    return render_template('time.html')


@app.route('/options', methods=['POST', 'GET'])
def options():
    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    r = redis.StrictRedis(host=myHostname, port=6380, password=myPassword, ssl=True)
    start_time = time.time()
    num = int(request.form['num'])
    rows = []
    c = []
    for i in range(num):
        val = round(random.uniform(2,5),1)
        cur = cnxn.cursor()
        a = 'select * from all_month WHERE mag = '+str(val)
       #cur.execute("select * from all_month WHERE mag = ?" ,(val,))
        v = str(val)
        if r.get(a):
            print ('Cached')
            c.append('Cached')
            print (r.get(a))
            rows.append(r.get(a))
        else:
            print('Not Cached')
            c.append('Not Cached')
            cur.execute("select * from all_month WHERE mag = ?" ,(val,))
            get = cur.fetchall();
            rows.append(get)
            r.set(a,str(get))
    end_time = time.time()
    elapsed_time = end_time - start_time
    r.flushdb()
    return render_template("list1.html", rows=[c], etime=elapsed_time)


@app.route('/options2', methods=['POST', 'GET'])
def options2():
    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    r = redis.StrictRedis(host=myHostname, port=6380, password=myPassword, ssl=True)
    start_time = time.time()
    num = int(request.form['num'])
    loc = (request.form['loc'])
    rows = []
    for i in range(num):
        cur = cnxn.cursor()
        b = "select * from all_month WHERE place LIKE '%'"+loc+"%"
       # cur.execute("select * from all_month WHERE place LIKE ?", ('%'+loc+'%',))
        if r.get(b):
            print('Cached')
            rows.append(r.get(b))
        else:
            print('Not Cached')
            cur.execute("select * from all_month WHERE place LIKE ?", ('%' + loc + '%',))
            get = cur.fetchall();
            rows.append(get)
            r.set(b,str(get))
    end_time = time.time()
    elapsed_time = end_time - start_time
    return render_template("list2.html", rows= [rows,elapsed_time])


@app.route('/options3', methods=['POST', 'GET'])
def options3():
    cnxn = pyodbc.connect( 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    r = redis.StrictRedis(host=myHostname, port=6380, password=myPassword, ssl=True)
    start_time = time.time()
    mag1 = float(request.form['mag1'])
    mag2 = float(request.form['mag2'])
    rows = []
    num = int(request.form['num'])
    for i in range(num):
        val=str(round(random.uniform(mag1,mag2),1))
        cur = cnxn.cursor()
        c = "select * from all_month WHERE mag="+val
       # cur.execute("select * from all_month WHERE place LIKE ?", ('%'+loc+'%',))
        if r.get(c):
            print('Cached')
            rows.append(r.get(c))
        else:
            print('Not Cached')
            cur.execute("select * from all_month WHERE mag between ? and ?", (mag1, mag2))
            get = cur.fetchall();
            rows.append(get)
            r.set(c,str(get))
    end_time = time.time()
    elapsed_time = end_time - start_time
    return render_template("list3.html", rows=[rows,elapsed_time])


@app.route('/distance',methods = ['POST', 'GET'])
def distance():
   cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
   r = redis.StrictRedis(host=myHostname, port=6380, password=myPassword, ssl=True)
   start_time = time.time()
   print (float(request.form['lat1']))
   print (float(request.form['lon1']))
   print (float(request.form['kms']))
   lat1 = float(request.form['lat1'])
   lon1 = float(request.form['lon1'])
   cur = cnxn.cursor()
   b = "select * from all_month where latitude  = "+request.form['lat1']+" and longitude = "+request.form['lon1']+"'"
   #cur.execute("select * from all_months ")
   #rows = cur.fetchall()
   #ref:https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude?utm_medium=organic&utm_source=google_rich_qa&utm_campaign=google_rich_qa
   R = 6373.0

   dist =[]
   rows = []
   if r.get(b):
       if(r.get(b) <= (float(request.form['kms']))):
           print("cache")

   else:
       cur.execute("select * from all_month where latitude  = ? and longitude = ?",(request.form['lat1'], request.form['lon1'],))
       data = cur.fetchall()
       lat_r = radians(float(request.form['lat1']))
       lon_r = radians(float(request.form['lon1']))
       for row in data:
           lat2 = radians(row[2])
           lon2 = radians(row[3])
           dlon = lon2 - lon1
           dlat = lat2 - lat1
           a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
           c = 2 * atan2(sqrt(a), sqrt(1 - a))
           distance = float(R * c)
           if (distance <= (float(request.form['kms']))):
               dist.append(row)
               r.set(b, distance)
           #rows.append(get)
   end_time = time.time()
   elapsed_time = end_time - start_time
   return render_template("list4.html", elapsed_time=elapsed_time, rows1=dist)



@app.route('/otr',methods=['POST', 'GET'])
def otr():
    cnxn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    r = redis.StrictRedis(host=myHostname, port=6380, password=myPassword, ssl=True)
    start_time = time.time()
    num = int(request.form['num'])
    time1 = request.form['time1']
    time2 = request.form['time2']
    rows = []
    for i in range(num):
        cur = cnxn.cursor()
        b = "select * from all_month WHERE time BETWEEN "+ time1 +" and " + time2
        # cur.execute("select * from all_month WHERE time BETWEEN ? and ?", (time1,time2))
        print(rows)
        if r.get(b):
            print('Cached')
            rows.append(r.get(b))
        else:
            print('Not Cached')
            cur.execute("select * from all_month WHERE time BETWEEN ? and ?", (time1,time2))
            get = cur.fetchall();
            rows.append(get)
            r.set(b, str(get))
    end_time = time.time()
    elapsed_time = end_time - start_time
    return render_template("list5.html", dist=rows, etime=elapsed_time)

if __name__ == '__main__':
    #app.run()
    app.run(host='0.0.0.0',port=port,debug=True)
    #r.flushdb()
