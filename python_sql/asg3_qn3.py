# -*- coding: utf-8 -*-

import sqlite3
import os
import pandas as pd

#change directory to where the files are stored
#os.chdir("C:/Users/chery/ST2195/st2195_assignment_3")
directory = "C:/Users/chery/ST2195/st2195_assignment_3/"


#Remove existing database
if os.path.exists("airline2.db"):
    os.remove("airline2.db")
else:
    pass
#try: 
#    os.remove("airline2.db")
#except OSError:
#    pass

#Create connection to database
connect = sqlite3.connect('airline2.db')

"Read csv files as data frames"
airports  = pd.read_csv(directory + "airports.csv")
carriers = pd.read_csv(directory + "carriers.csv")
planes = pd.read_csv(directory + "plane-data.csv")

path = os.getcwd()
for i in range(2000, 2006):  #2006 is not included
        ontime = pd.read_csv(directory + str(i)+ ".csv")
        ontime.to_sql('ontime', con = connect, if_exists = 'append', index = False)
        

#Copy data frames to database tables
airports.to_sql('airports', con = connect, index = False)
carriers.to_sql('carriers', con = connect, index = False)
planes.to_sql('planes', con = connect, index = False)

#create cursor object
c = connect.cursor()

#Executing SQL commands

#a.
c.execute('''
          SELECT model AS model, AVG(ontime.DepDelay) as avgDelay
                 FROM planes JOIN ontime USING(tailnum)
                 WHERE ontime.Cancelled = 0 AND Ontime.Diverted = 0 AND ontime.DepDelay > 0
                 GROUP BY model
                 ORDER BY avgDelay
          ''')
print(c.fetchone()[0], "has the lowest associated average depaerture delay.")
         
#b.
c.execute('''
          SELECT airports.city AS cities, COUNT(*) AS total
                 FROM airports JOIN ontime ON ontime.Dest = airports.iata
                 WHERE ontime.Cancelled = 0
                 GROUP BY cities
                 ORDER BY total DESC
          ''')
print(c.fetchone()[0], "has the highest number of inbound flights excluding cancelled flights.")

#c.
c.execute('''
          SELECT carriers.Description AS company, COUNT(*) AS total
                 FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
                 WHERE ontime.Cancelled = 1 AND company IN ('Delta Air Lines Inc.','United Air Lines Inc.','American Airlines Inc.','Pinnacle Airlines Inc.')
                 GROUP BY company
                 ORDER BY total DESC
          ''')                                                                                                                                                                                                              
print(c.fetchone()[0], "has the highest number of cancelled flights.")

#d.
c.execute('''
SELECT
q1.carrier AS carrier, (CAST(q1.numerator AS FLOAT)/CAST(q2.denominator AS FLOAT)) AS ratio
FROM
(
 SELECT carriers.Description AS carrier, count(*) AS numerator
 FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
 WHERE ontime.Cancelled = 1 AND carrier IN ('Delta Air Lines Inc.','United Air Lines Inc.','American Airlines Inc.','Pinnacle Airlines Inc.')
 GROUP BY carrier
) AS q1 JOIN
(
 SELECT carriers.Description as carrier, COUNT(*) AS denominator 
 FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
 WHERE carriers.Description IN ('Delta Air Lines Inc.','United Air Lines Inc.','American Airlines Inc.','Pinnacle Airlines Inc.')
 GROUP BY carriers.Description
) AS q2 USING(carrier)
ORDER BY ratio DESC
''')

print(c.fetchone()[0], "has the highest number of cancelled flights with respect to their total number of flights.")

#end connection
connect.close()