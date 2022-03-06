import math

import mysql.connector
import csv
import numpy
import pandas as pd
from tkinter import Tk
from tkinter.filedialog import askdirectory
from matplotlib import pyplot as plt


path = '/Users/mikailaras/Desktop/CSV/'
datei_ideal = 'ideal.csv'
datei_train = 'train.csv'
datei_test = 'test.csv'

#<Pfad aussuchen für anderen Computer>____________
#path = askdirectory(title='Wählen Sie den Ablageordner der CSV-Dateien aus') # shows dialog box and return the path

# datei_ideal = '/ideal.csv'
# datei_train = '/train.csv'
# datei_test = '/test.csv'

#</Pfad aussuchen für anderen Computer>___________

data_train = pd.read_csv (r'/Users/mikailaras/Desktop/CSV/train.csv')
df_train = pd.DataFrame(data_train)

data_test = pd.read_csv (r'/Users/mikailaras/Desktop/CSV/test.csv')
df_test = pd.DataFrame(data_test)

data_ideal = pd.read_csv (r'/Users/mikailaras/Desktop/CSV/ideal.csv')
df_ideal = pd.DataFrame(data_ideal)

config = {
     'user': 'root',
     'password': 'Lo34pg8g/new',
     'host': 'localhost',
     'database': 'dbschemaaras'
 }

db = mysql.connector.connect(**config)

cursor = db.cursor()

cursor.execute('''
		CREATE TABLE IF NOT EXISTS `DBSCHEMAARAS`.`train` (
  `x` DECIMAL(3,1) NOT NULL,
  `y1` DECIMAL(20,12) NULL,
  `y2` DECIMAL(20,12) NULL,
  `y3` DECIMAL(20,12) NULL,
  `y4` DECIMAL(20,12) NULL
  );
               ''')

result = cursor.fetchall()
cursor.close()
cursor = db.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS `DBSCHEMAARAS`.`ideal` (
  `x` DECIMAL(3,1) NOT NULL,
  `y1` DECIMAL(20,12) NULL,
  `y2` DECIMAL(20,12) NULL,
  `y3` DECIMAL(20,12) NULL,
  `y4` DECIMAL(20,12) NULL,
  `y5` DECIMAL(20,12) NULL,
  `y6` DECIMAL(20,12) NULL,
  `y7` DECIMAL(20,12) NULL,
  `y8` DECIMAL(20,12) NULL,
  `y9` DECIMAL(20,12) NULL,
  `y10` DECIMAL(20,12) NULL,
  `y11` DECIMAL(20,12) NULL,
  `y12` DECIMAL(20,12) NULL,
  `y13` DECIMAL(20,12) NULL,
  `y14` DECIMAL(20,12) NULL,
  `y15` DECIMAL(20,12) NULL,
  `y16` DECIMAL(20,12) NULL,
  `y17` DECIMAL(20,12) NULL,
  `y18` DECIMAL(20,12) NULL,
  `y19` DECIMAL(20,12) NULL,
  `y20` DECIMAL(20,12) NULL,
  `y21` DECIMAL(20,12) NULL,
  `y22` DECIMAL(20,12) NULL,
  `y23` DECIMAL(20,12) NULL,
  `y24` DECIMAL(20,12) NULL,
  `y25` DECIMAL(20,12) NULL,
  `y26` DECIMAL(20,12) NULL,
  `y27` DECIMAL(20,12) NULL,
  `y28` DECIMAL(20,12) NULL,
  `y29` DECIMAL(20,12) NULL,
  `y30` DECIMAL(20,12) NULL,
  `y31` DECIMAL(20,12) NULL,
  `y32` DECIMAL(20,12) NULL,
  `y33` DECIMAL(20,12) NULL,
  `y34` DECIMAL(20,12) NULL,
  `y35` DECIMAL(20,12) NULL,
  `y36` DECIMAL(20,12) NULL,
  `y37` DECIMAL(20,12) NULL,
  `y38` DECIMAL(20,12) NULL,
  `y39` DECIMAL(20,12) NULL,
  `y40` DECIMAL(20,12) NULL,
  `y41` DECIMAL(20,12) NULL,
  `y42` DECIMAL(20,12) NULL,
  `y43` DECIMAL(20,12) NULL,
  `y44` DECIMAL(20,12) NULL,
  `y45` DECIMAL(20,12) NULL,
  `y46` DECIMAL(20,12) NULL,
  `y47` DECIMAL(20,12) NULL,
  `y48` DECIMAL(20,12) NULL,
  `y49` DECIMAL(20,12) NULL,
  `y50` DECIMAL(20,12) NULL
  );
   ''')

result = cursor.fetchall()
cursor.close()
cursor = db.cursor()

cursor.execute('''
    CREATE TABLE IF NOT EXISTS `DBSCHEMAARAS`.`test` (
  `x` DECIMAL(3,1) NOT NULL,
  `y` DECIMAL(20,12) NULL
  ); ''')

result = cursor.fetchall()
cursor.close()
cursor = db.cursor()


for row in df_train.itertuples():
    statement = 'INSERT INTO dbschemaaras.train (x, y1, y2, y3, y4) VALUES (%s,%s,%s,%s,%s)'
    data = (row[1],row[2],row[3],row[4],row[5])
    cursor.execute(statement, data)
db.commit()

result = cursor.fetchall()
cursor.close()
cursor = db.cursor()

for row in df_ideal.itertuples():
    statement = 'INSERT INTO dbschemaaras.ideal (x, y1, y2, y3, y4,y5,y6,y7,y8,y9,y10,y11,y12,y13,y14,y15,y16,y17,y18,y19,y20,y21,y22,y23,y24,y25,y26,y27,y28,y29,y30,y31,y32,y33,y34,y35,y36,y37,y38,y39,y40,y41,y42,y43,y44,y45,y46,y47,y48,y49,y50) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    data = (row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23],row[24],row[25],row[26],row[27],row[28],row[29],row[30],row[31],row[32],row[33],row[34],row[35],row[36],row[37],row[38],row[39],row[40],row[41],row[42],row[43],row[44],row[45],row[46],row[47],row[48],row[49],row[50],row[51])
    cursor.execute(statement, data)
db.commit()

result = cursor.fetchall()
cursor.close()
cursor = db.cursor()

for row in df_test.itertuples():
    statement = 'INSERT INTO dbschemaaras.test (x, y) VALUES (%s,%s)'
    data = (row[1],row[2])
    cursor.execute(statement, data)
db.commit()

result = cursor.fetchall()
cursor.close()
cursor = db.cursor()

cursor.execute('''
CREATE TABLE distanz AS SELECT 
t.y1 - i.y1 as "y1_y1",
t.y1 - i.y2 as "y1_y2",
t.y1 - i.y3 as "y1_y3",
t.y1 - i.y4 as "y1_y4",
t.y1 - i.y5 as "y1_y5",
t.y1 - i.y6 as "y1_y6",
t.y1 - i.y7 as "y1_y7",
t.y1 - i.y8 as "y1_y8",
t.y1 - i.y9 as "y1_y9",
t.y1 - i.y10 as "y1_y10",
t.y1 - i.y11 as "y1_y11",
t.y1 - i.y12 as "y1_y12",
t.y1 - i.y13 as "y1_y13",
t.y1 - i.y14 as "y1_y14",
t.y1 - i.y15 as "y1_y15",
t.y1 - i.y16 as "y1_y16",
t.y1 - i.y17 as "y1_y17",
t.y1 - i.y18 as "y1_y18",
t.y1 - i.y19 as "y1_y19",
t.y1 - i.y20 as "y1_y20",
t.y1 - i.y21 as "y1_y21",
t.y1 - i.y22 as "y1_y22",
t.y1 - i.y23 as "y1_y23",
t.y1 - i.y24 as "y1_y24",
t.y1 - i.y25 as "y1_y25",
t.y1 - i.y26 as "y1_y26",
t.y1 - i.y27 as "y1_y27",
t.y1 - i.y28 as "y1_y28",
t.y1 - i.y29 as "y1_y29",
t.y1 - i.y30 as "y1_y30",
t.y1 - i.y31 as "y1_y31",
t.y1 - i.y32 as "y1_y32",
t.y1 - i.y33 as "y1_y33",
t.y1 - i.y34 as "y1_y34",
t.y1 - i.y35 as "y1_y35",
t.y1 - i.y36 as "y1_y36",
t.y1 - i.y37 as "y1_y37",
t.y1 - i.y38 as "y1_y38",
t.y1 - i.y39 as "y1_y39",
t.y1 - i.y40 as "y1_y40",
t.y1 - i.y41 as "y1_y41",
t.y1 - i.y42 as "y1_y42",
t.y1 - i.y43 as "y1_y43",
t.y1 - i.y44 as "y1_y44",
t.y1 - i.y45 as "y1_y45",
t.y1 - i.y46 as "y1_y46",
t.y1 - i.y47 as "y1_y47",
t.y1 - i.y48 as "y1_y48",
t.y1 - i.y49 as "y1_y49",
t.y1 - i.y50 as "y1_y50",
t.y2 - i.y1 as "y2_y1",
t.y2 - i.y2 as "y2_y2",
t.y2 - i.y3 as "y2_y3",
t.y2 - i.y4 as "y2_y4",
t.y2 - i.y5 as "y2_y5",
t.y2 - i.y6 as "y2_y6",
t.y2 - i.y7 as "y2_y7",
t.y2 - i.y8 as "y2_y8",
t.y2 - i.y9 as "y2_y9",
t.y2 - i.y10 as "y2_y10",
t.y2 - i.y11 as "y2_y11",
t.y2 - i.y12 as "y2_y12",
t.y2 - i.y13 as "y2_y13",
t.y2 - i.y14 as "y2_y14",
t.y2 - i.y15 as "y2_y15",
t.y2 - i.y16 as "y2_y16",
t.y2 - i.y17 as "y2_y17",
t.y2 - i.y18 as "y2_y18",
t.y2 - i.y19 as "y2_y19",
t.y2 - i.y20 as "y2_y20",
t.y2 - i.y21 as "y2_y21",
t.y2 - i.y22 as "y2_y22",
t.y2 - i.y23 as "y2_y23",
t.y2 - i.y24 as "y2_y24",
t.y2 - i.y25 as "y2_y25",
t.y2 - i.y26 as "y2_y26",
t.y2 - i.y27 as "y2_y27",
t.y2 - i.y28 as "y2_y28",
t.y2 - i.y29 as "y2_y29",
t.y2 - i.y30 as "y2_y30",
t.y2 - i.y31 as "y2_y31",
t.y2 - i.y32 as "y2_y32",
t.y2 - i.y33 as "y2_y33",
t.y2 - i.y34 as "y2_y34",
t.y2 - i.y35 as "y2_y35",
t.y2 - i.y36 as "y2_y36",
t.y2 - i.y37 as "y2_y37",
t.y2 - i.y38 as "y2_y38",
t.y2 - i.y39 as "y2_y39",
t.y2 - i.y40 as "y2_y40",
t.y2 - i.y41 as "y2_y41",
t.y2 - i.y42 as "y2_y42",
t.y2 - i.y43 as "y2_y43",
t.y2 - i.y44 as "y2_y44",
t.y2 - i.y45 as "y2_y45",
t.y2 - i.y46 as "y2_y46",
t.y2 - i.y47 as "y2_y47",
t.y2 - i.y48 as "y2_y48",
t.y2 - i.y49 as "y2_y49",
t.y2 - i.y50 as "y2_y50",
t.y3 - i.y1 as "y3_y1",
t.y3 - i.y2 as "y3_y2",
t.y3 - i.y3 as "y3_y3",
t.y3 - i.y4 as "y3_y4",
t.y3 - i.y5 as "y3_y5",
t.y3 - i.y6 as "y3_y6",
t.y3 - i.y7 as "y3_y7",
t.y3 - i.y8 as "y3_y8",
t.y3 - i.y9 as "y3_y9",
t.y3 - i.y10 as "y3_y10",
t.y3 - i.y11 as "y3_y11",
t.y3 - i.y12 as "y3_y12",
t.y3 - i.y13 as "y3_y13",
t.y3 - i.y14 as "y3_y14",
t.y3 - i.y15 as "y3_y15",
t.y3 - i.y16 as "y3_y16",
t.y3 - i.y17 as "y3_y17",
t.y3 - i.y18 as "y3_y18",
t.y3 - i.y19 as "y3_y19",
t.y3 - i.y20 as "y3_y20",
t.y3 - i.y21 as "y3_y21",
t.y3 - i.y22 as "y3_y22",
t.y3 - i.y23 as "y3_y23",
t.y3 - i.y24 as "y3_y24",
t.y3 - i.y25 as "y3_y25",
t.y3 - i.y26 as "y3_y26",
t.y3 - i.y27 as "y3_y27",
t.y3 - i.y28 as "y3_y28",
t.y3 - i.y29 as "y3_y29",
t.y3 - i.y30 as "y3_y30",
t.y3 - i.y31 as "y3_y31",
t.y3 - i.y32 as "y3_y32",
t.y3 - i.y33 as "y3_y33",
t.y3 - i.y34 as "y3_y34",
t.y3 - i.y35 as "y3_y35",
t.y3 - i.y36 as "y3_y36",
t.y3 - i.y37 as "y3_y37",
t.y3 - i.y38 as "y3_y38",
t.y3 - i.y39 as "y3_y39",
t.y3 - i.y40 as "y3_y40",
t.y3 - i.y41 as "y3_y41",
t.y3 - i.y42 as "y3_y42",
t.y3 - i.y43 as "y3_y43",
t.y3 - i.y44 as "y3_y44",
t.y3 - i.y45 as "y3_y45",
t.y3 - i.y46 as "y3_y46",
t.y3 - i.y47 as "y3_y47",
t.y3 - i.y48 as "y3_y48",
t.y3 - i.y49 as "y3_y49",
t.y3 - i.y50 as "y3_y50",
t.y4 - i.y1 as "y4_y1",
t.y4 - i.y2 as "y4_y2",
t.y4 - i.y3 as "y4_y3",
t.y4 - i.y4 as "y4_y4",
t.y4 - i.y5 as "y4_y5",
t.y4 - i.y6 as "y4_y6",
t.y4 - i.y7 as "y4_y7",
t.y4 - i.y8 as "y4_y8",
t.y4 - i.y9 as "y4_y9",
t.y4 - i.y10 as "y4_y10",
t.y4 - i.y11 as "y4_y11",
t.y4 - i.y12 as "y4_y12",
t.y4 - i.y13 as "y4_y13",
t.y4 - i.y14 as "y4_y14",
t.y4 - i.y15 as "y4_y15",
t.y4 - i.y16 as "y4_y16",
t.y4 - i.y17 as "y4_y17",
t.y4 - i.y18 as "y4_y18",
t.y4 - i.y19 as "y4_y19",
t.y4 - i.y20 as "y4_y20",
t.y4 - i.y21 as "y4_y21",
t.y4 - i.y22 as "y4_y22",
t.y4 - i.y23 as "y4_y23",
t.y4 - i.y24 as "y4_y24",
t.y4 - i.y25 as "y4_y25",
t.y4 - i.y26 as "y4_y26",
t.y4 - i.y27 as "y4_y27",
t.y4 - i.y28 as "y4_y28",
t.y4 - i.y29 as "y4_y29",
t.y4 - i.y30 as "y4_y30",
t.y4 - i.y31 as "y4_y31",
t.y4 - i.y32 as "y4_y32",
t.y4 - i.y33 as "y4_y33",
t.y4 - i.y34 as "y4_y34",
t.y4 - i.y35 as "y4_y35",
t.y4 - i.y36 as "y4_y36",
t.y4 - i.y37 as "y4_y37",
t.y4 - i.y38 as "y4_y38",
t.y4 - i.y39 as "y4_y39",
t.y4 - i.y40 as "y4_y40",
t.y4 - i.y41 as "y4_y41",
t.y4 - i.y42 as "y4_y42",
t.y4 - i.y43 as "y4_y43",
t.y4 - i.y44 as "y4_y44",
t.y4 - i.y45 as "y4_y45",
t.y4 - i.y46 as "y4_y46",
t.y4 - i.y47 as "y4_y47",
t.y4 - i.y48 as "y4_y48",
t.y4 - i.y49 as "y4_y49",
t.y4 - i.y50 as "y4_y50"
FROM DBSCHEMAARAS.ideal as i
LEFT OUTER JOIN DBSCHEMAARAS.train as t on t.x = i.x
''')

result = cursor.fetchall()


cursor.close()
cursor = db.cursor()


cursor.execute('''
CREATE TABLE quad_distanz AS SELECT 
SUM(POWER((y1_y1),2)) as "y1_y1",
SUM(POWER((y1_y2),2)) as "y1_y2",
SUM(POWER((y1_y3),2)) as "y1_y3",
SUM(POWER((y1_y4),2)) as "y1_y4",
SUM(POWER((y1_y5),2)) as "y1_y5",
SUM(POWER((y1_y6),2)) as "y1_y6",
SUM(POWER((y1_y7),2)) as "y1_y7",
SUM(POWER((y1_y8),2)) as "y1_y8",
SUM(POWER((y1_y9),2)) as "y1_y9",
SUM(POWER((y1_y10),2)) as "y1_y10",
SUM(POWER((y1_y11),2)) as "y1_y11",
SUM(POWER((y1_y12),2)) as "y1_y12",
SUM(POWER((y1_y13),2)) as "y1_y13",
SUM(POWER((y1_y14),2)) as "y1_y14",
SUM(POWER((y1_y15),2)) as "y1_y15",
SUM(POWER((y1_y16),2)) as "y1_y16",
SUM(POWER((y1_y17),2)) as "y1_y17",
SUM(POWER((y1_y18),2)) as "y1_y18",
SUM(POWER((y1_y19),2)) as "y1_y19",
SUM(POWER((y1_y20),2)) as "y1_y20",
SUM(POWER((y1_y21),2)) as "y1_y21",
SUM(POWER((y1_y22),2)) as "y1_y22",
SUM(POWER((y1_y23),2)) as "y1_y23",
SUM(POWER((y1_y24),2)) as "y1_y24",
SUM(POWER((y1_y25),2)) as "y1_y25",
SUM(POWER((y1_y26),2)) as "y1_y26",
SUM(POWER((y1_y27),2)) as "y1_y27",
SUM(POWER((y1_y28),2)) as "y1_y28",
SUM(POWER((y1_y29),2)) as "y1_y29",
SUM(POWER((y1_y30),2)) as "y1_y30",
SUM(POWER((y1_y31),2)) as "y1_y31",
SUM(POWER((y1_y32),2)) as "y1_y32",
SUM(POWER((y1_y33),2)) as "y1_y33",
SUM(POWER((y1_y34),2)) as "y1_y34",
SUM(POWER((y1_y35),2)) as "y1_y35",
SUM(POWER((y1_y36),2)) as "y1_y36",
SUM(POWER((y1_y37),2)) as "y1_y37",
SUM(POWER((y1_y38),2)) as "y1_y38",
SUM(POWER((y1_y39),2)) as "y1_y39",
SUM(POWER((y1_y40),2)) as "y1_y40",
SUM(POWER((y1_y41),2)) as "y1_y41",
SUM(POWER((y1_y42),2)) as "y1_y42",
SUM(POWER((y1_y43),2)) as "y1_y43",
SUM(POWER((y1_y44),2)) as "y1_y44",
SUM(POWER((y1_y45),2)) as "y1_y45",
SUM(POWER((y1_y46),2)) as "y1_y46",
SUM(POWER((y1_y47),2)) as "y1_y47",
SUM(POWER((y1_y48),2)) as "y1_y48",
SUM(POWER((y1_y49),2)) as "y1_y49",
SUM(POWER((y1_y50),2)) as "y1_y50",
SUM(POWER((y2_y1),2)) as "y2_y1",
SUM(POWER((y2_y2),2)) as "y2_y2",
SUM(POWER((y2_y3),2)) as "y2_y3",
SUM(POWER((y2_y4),2)) as "y2_y4",
SUM(POWER((y2_y5),2)) as "y2_y5",
SUM(POWER((y2_y6),2)) as "y2_y6",
SUM(POWER((y2_y7),2)) as "y2_y7",
SUM(POWER((y2_y8),2)) as "y2_y8",
SUM(POWER((y2_y9),2)) as "y2_y9",
SUM(POWER((y2_y10),2)) as "y2_y10",
SUM(POWER((y2_y11),2)) as "y2_y11",
SUM(POWER((y2_y12),2)) as "y2_y12",
SUM(POWER((y2_y13),2)) as "y2_y13",
SUM(POWER((y2_y14),2)) as "y2_y14",
SUM(POWER((y2_y15),2)) as "y2_y15",
SUM(POWER((y2_y16),2)) as "y2_y16",
SUM(POWER((y2_y17),2)) as "y2_y17",
SUM(POWER((y2_y18),2)) as "y2_y18",
SUM(POWER((y2_y19),2)) as "y2_y19",
SUM(POWER((y2_y20),2)) as "y2_y20",
SUM(POWER((y2_y21),2)) as "y2_y21",
SUM(POWER((y2_y22),2)) as "y2_y22",
SUM(POWER((y2_y23),2)) as "y2_y23",
SUM(POWER((y2_y24),2)) as "y2_y24",
SUM(POWER((y2_y25),2)) as "y2_y25",
SUM(POWER((y2_y26),2)) as "y2_y26",
SUM(POWER((y2_y27),2)) as "y2_y27",
SUM(POWER((y2_y28),2)) as "y2_y28",
SUM(POWER((y2_y29),2)) as "y2_y29",
SUM(POWER((y2_y30),2)) as "y2_y30",
SUM(POWER((y2_y31),2)) as "y2_y31",
SUM(POWER((y2_y32),2)) as "y2_y32",
SUM(POWER((y2_y33),2)) as "y2_y33",
SUM(POWER((y2_y34),2)) as "y2_y34",
SUM(POWER((y2_y35),2)) as "y2_y35",
SUM(POWER((y2_y36),2)) as "y2_y36",
SUM(POWER((y2_y37),2)) as "y2_y37",
SUM(POWER((y2_y38),2)) as "y2_y38",
SUM(POWER((y2_y39),2)) as "y2_y39",
SUM(POWER((y2_y40),2)) as "y2_y40",
SUM(POWER((y2_y41),2)) as "y2_y41",
SUM(POWER((y2_y42),2)) as "y2_y42",
SUM(POWER((y2_y43),2)) as "y2_y43",
SUM(POWER((y2_y44),2)) as "y2_y44",
SUM(POWER((y2_y45),2)) as "y2_y45",
SUM(POWER((y2_y46),2)) as "y2_y46",
SUM(POWER((y2_y47),2)) as "y2_y47",
SUM(POWER((y2_y48),2)) as "y2_y48",
SUM(POWER((y2_y49),2)) as "y2_y49",
SUM(POWER((y2_y50),2)) as "y2_y50",
SUM(POWER((y3_y1),2)) as "y3_y1",
SUM(POWER((y3_y2),2)) as "y3_y2",
SUM(POWER((y3_y3),2)) as "y3_y3",
SUM(POWER((y3_y4),2)) as "y3_y4",
SUM(POWER((y3_y5),2)) as "y3_y5",
SUM(POWER((y3_y6),2)) as "y3_y6",
SUM(POWER((y3_y7),2)) as "y3_y7",
SUM(POWER((y3_y8),2)) as "y3_y8",
SUM(POWER((y3_y9),2)) as "y3_y9",
SUM(POWER((y3_y10),2)) as "y3_y10",
SUM(POWER((y3_y11),2)) as "y3_y11",
SUM(POWER((y3_y12),2)) as "y3_y12",
SUM(POWER((y3_y13),2)) as "y3_y13",
SUM(POWER((y3_y14),2)) as "y3_y14",
SUM(POWER((y3_y15),2)) as "y3_y15",
SUM(POWER((y3_y16),2)) as "y3_y16",
SUM(POWER((y3_y17),2)) as "y3_y17",
SUM(POWER((y3_y18),2)) as "y3_y18",
SUM(POWER((y3_y19),2)) as "y3_y19",
SUM(POWER((y3_y20),2)) as "y3_y20",
SUM(POWER((y3_y21),2)) as "y3_y21",
SUM(POWER((y3_y22),2)) as "y3_y22",
SUM(POWER((y3_y23),2)) as "y3_y23",
SUM(POWER((y3_y24),2)) as "y3_y24",
SUM(POWER((y3_y25),2)) as "y3_y25",
SUM(POWER((y3_y26),2)) as "y3_y26",
SUM(POWER((y3_y27),2)) as "y3_y27",
SUM(POWER((y3_y28),2)) as "y3_y28",
SUM(POWER((y3_y29),2)) as "y3_y29",
SUM(POWER((y3_y30),2)) as "y3_y30",
SUM(POWER((y3_y31),2)) as "y3_y31",
SUM(POWER((y3_y32),2)) as "y3_y32",
SUM(POWER((y3_y33),2)) as "y3_y33",
SUM(POWER((y3_y34),2)) as "y3_y34",
SUM(POWER((y3_y35),2)) as "y3_y35",
SUM(POWER((y3_y36),2)) as "y3_y36",
SUM(POWER((y3_y37),2)) as "y3_y37",
SUM(POWER((y3_y38),2)) as "y3_y38",
SUM(POWER((y3_y39),2)) as "y3_y39",
SUM(POWER((y3_y40),2)) as "y3_y40",
SUM(POWER((y3_y41),2)) as "y3_y41",
SUM(POWER((y3_y42),2)) as "y3_y42",
SUM(POWER((y3_y43),2)) as "y3_y43",
SUM(POWER((y3_y44),2)) as "y3_y44",
SUM(POWER((y3_y45),2)) as "y3_y45",
SUM(POWER((y3_y46),2)) as "y3_y46",
SUM(POWER((y3_y47),2)) as "y3_y47",
SUM(POWER((y3_y48),2)) as "y3_y48",
SUM(POWER((y3_y49),2)) as "y3_y49",
SUM(POWER((y3_y50),2)) as "y3_y50",
SUM(POWER((y4_y1),2)) as "y4_y1",
SUM(POWER((y4_y2),2)) as "y4_y2",
SUM(POWER((y4_y3),2)) as "y4_y3",
SUM(POWER((y4_y4),2)) as "y4_y4",
SUM(POWER((y4_y5),2)) as "y4_y5",
SUM(POWER((y4_y6),2)) as "y4_y6",
SUM(POWER((y4_y7),2)) as "y4_y7",
SUM(POWER((y4_y8),2)) as "y4_y8",
SUM(POWER((y4_y9),2)) as "y4_y9",
SUM(POWER((y4_y10),2)) as "y4_y10",
SUM(POWER((y4_y11),2)) as "y4_y11",
SUM(POWER((y4_y12),2)) as "y4_y12",
SUM(POWER((y4_y13),2)) as "y4_y13",
SUM(POWER((y4_y14),2)) as "y4_y14",
SUM(POWER((y4_y15),2)) as "y4_y15",
SUM(POWER((y4_y16),2)) as "y4_y16",
SUM(POWER((y4_y17),2)) as "y4_y17",
SUM(POWER((y4_y18),2)) as "y4_y18",
SUM(POWER((y4_y19),2)) as "y4_y19",
SUM(POWER((y4_y20),2)) as "y4_y20",
SUM(POWER((y4_y21),2)) as "y4_y21",
SUM(POWER((y4_y22),2)) as "y4_y22",
SUM(POWER((y4_y23),2)) as "y4_y23",
SUM(POWER((y4_y24),2)) as "y4_y24",
SUM(POWER((y4_y25),2)) as "y4_y25",
SUM(POWER((y4_y26),2)) as "y4_y26",
SUM(POWER((y4_y27),2)) as "y4_y27",
SUM(POWER((y4_y28),2)) as "y4_y28",
SUM(POWER((y4_y29),2)) as "y4_y29",
SUM(POWER((y4_y30),2)) as "y4_y30",
SUM(POWER((y4_y31),2)) as "y4_y31",
SUM(POWER((y4_y32),2)) as "y4_y32",
SUM(POWER((y4_y33),2)) as "y4_y33",
SUM(POWER((y4_y34),2)) as "y4_y34",
SUM(POWER((y4_y35),2)) as "y4_y35",
SUM(POWER((y4_y36),2)) as "y4_y36",
SUM(POWER((y4_y37),2)) as "y4_y37",
SUM(POWER((y4_y38),2)) as "y4_y38",
SUM(POWER((y4_y39),2)) as "y4_y39",
SUM(POWER((y4_y40),2)) as "y4_y40",
SUM(POWER((y4_y41),2)) as "y4_y41",
SUM(POWER((y4_y42),2)) as "y4_y42",
SUM(POWER((y4_y43),2)) as "y4_y43",
SUM(POWER((y4_y44),2)) as "y4_y44",
SUM(POWER((y4_y45),2)) as "y4_y45",
SUM(POWER((y4_y46),2)) as "y4_y46",
SUM(POWER((y4_y47),2)) as "y4_y47",
SUM(POWER((y4_y48),2)) as "y4_y48",
SUM(POWER((y4_y49),2)) as "y4_y49",
SUM(POWER((y4_y50),2)) as "y4_y50"
FROM DBSCHEMAARAS.distanz
''')

result = cursor.fetchall()


cursor.close()
cursor = db.cursor()

#<Identifikation der Idealen Funktionen>------------------------------

cursor.execute('''
CREATE TABLE zuweisung_ideale AS SELECT CASE LEAST(
y1_y1,
y1_y2,
y1_y3,
y1_y4,
y1_y5,
y1_y6,
y1_y7,
y1_y8,
y1_y9,
y1_y10,
y1_y11,
y1_y12,
y1_y13,
y1_y14,
y1_y15,
y1_y16,
y1_y17,
y1_y18,
y1_y19,
y1_y20,
y1_y21,
y1_y22,
y1_y23,
y1_y24,
y1_y25,
y1_y26,
y1_y27,
y1_y28,
y1_y29,
y1_y30,
y1_y31,
y1_y32,
y1_y33,
y1_y34,
y1_y35,
y1_y36,
y1_y37,
y1_y38,
y1_y39,
y1_y40,
y1_y41,
y1_y42,
y1_y43,
y1_y44,
y1_y45,
y1_y46,
y1_y47,
y1_y48,
y1_y49,
y1_y50)
WHEN y1_y1 THEN "y1"
WHEN y1_y2 THEN "y2"
WHEN y1_y3 THEN "y3"
WHEN y1_y4 THEN "y4"
WHEN y1_y5 THEN "y5"
WHEN y1_y6 THEN "y6"
WHEN y1_y7 THEN "y7"
WHEN y1_y8 THEN "y8"
WHEN y1_y9 THEN "y9"
WHEN y1_y10 THEN "y10"
WHEN y1_y11 THEN "y11"
WHEN y1_y12 THEN "y12"
WHEN y1_y13 THEN "y13"
WHEN y1_y14 THEN "y14"
WHEN y1_y15 THEN "y15"
WHEN y1_y16 THEN "y16"
WHEN y1_y17 THEN "y17"
WHEN y1_y18 THEN "y18"
WHEN y1_y19 THEN "y19"
WHEN y1_y20 THEN "y20"
WHEN y1_y21 THEN "y21"
WHEN y1_y22 THEN "y22"
WHEN y1_y23 THEN "y23"
WHEN y1_y24 THEN "y24"
WHEN y1_y25 THEN "y25"
WHEN y1_y26 THEN "y26"
WHEN y1_y27 THEN "y27"
WHEN y1_y28 THEN "y28"
WHEN y1_y29 THEN "y29"
WHEN y1_y30 THEN "y30"
WHEN y1_y31 THEN "y31"
WHEN y1_y32 THEN "y32"
WHEN y1_y33 THEN "y33"
WHEN y1_y34 THEN "y34"
WHEN y1_y35 THEN "y35"
WHEN y1_y36 THEN "y36"
WHEN y1_y37 THEN "y37"
WHEN y1_y38 THEN "y38"
WHEN y1_y39 THEN "y39"
WHEN y1_y40 THEN "y40"
WHEN y1_y41 THEN "y41"
WHEN y1_y42 THEN "y42"
WHEN y1_y43 THEN "y43"
WHEN y1_y44 THEN "y44"
WHEN y1_y45 THEN "y45"
WHEN y1_y46 THEN "y46"
WHEN y1_y47 THEN "y47"
WHEN y1_y48 THEN "y48"
WHEN y1_y49 THEN "y49"
WHEN y1_y50 THEN "y50"
END AS LEAST_Y1,
CASE LEAST(
y2_y1,
y2_y2,
y2_y3,
y2_y4,
y2_y5,
y2_y6,
y2_y7,
y2_y8,
y2_y9,
y2_y10,
y2_y11,
y2_y12,
y2_y13,
y2_y14,
y2_y15,
y2_y16,
y2_y17,
y2_y18,
y2_y19,
y2_y20,
y2_y21,
y2_y22,
y2_y23,
y2_y24,
y2_y25,
y2_y26,
y2_y27,
y2_y28,
y2_y29,
y2_y30,
y2_y31,
y2_y32,
y2_y33,
y2_y34,
y2_y35,
y2_y36,
y2_y37,
y2_y38,
y2_y39,
y2_y40,
y2_y41,
y2_y42,
y2_y43,
y2_y44,
y2_y45,
y2_y46,
y2_y47,
y2_y48,
y2_y49,
y2_y50)
WHEN y2_y1 THEN "y1"
WHEN y2_y2 THEN "y2"
WHEN y2_y3 THEN "y3"
WHEN y2_y4 THEN "y4"
WHEN y2_y5 THEN "y5"
WHEN y2_y6 THEN "y6"
WHEN y2_y7 THEN "y7"
WHEN y2_y8 THEN "y8"
WHEN y2_y9 THEN "y9"
WHEN y2_y10 THEN "y10"
WHEN y2_y11 THEN "y11"
WHEN y2_y12 THEN "y12"
WHEN y2_y13 THEN "y13"
WHEN y2_y14 THEN "y14"
WHEN y2_y15 THEN "y15"
WHEN y2_y16 THEN "y16"
WHEN y2_y17 THEN "y17"
WHEN y2_y18 THEN "y18"
WHEN y2_y19 THEN "y19"
WHEN y2_y20 THEN "y20"
WHEN y2_y21 THEN "y21"
WHEN y2_y22 THEN "y22"
WHEN y2_y23 THEN "y23"
WHEN y2_y24 THEN "y24"
WHEN y2_y25 THEN "y25"
WHEN y2_y26 THEN "y26"
WHEN y2_y27 THEN "y27"
WHEN y2_y28 THEN "y28"
WHEN y2_y29 THEN "y29"
WHEN y2_y30 THEN "y30"
WHEN y2_y31 THEN "y31"
WHEN y2_y32 THEN "y32"
WHEN y2_y33 THEN "y33"
WHEN y2_y34 THEN "y34"
WHEN y2_y35 THEN "y35"
WHEN y2_y36 THEN "y36"
WHEN y2_y37 THEN "y37"
WHEN y2_y38 THEN "y38"
WHEN y2_y39 THEN "y39"
WHEN y2_y40 THEN "y40"
WHEN y2_y41 THEN "y41"
WHEN y2_y42 THEN "y42"
WHEN y2_y43 THEN "y43"
WHEN y2_y44 THEN "y44"
WHEN y2_y45 THEN "y45"
WHEN y2_y46 THEN "y46"
WHEN y2_y47 THEN "y47"
WHEN y2_y48 THEN "y48"
WHEN y2_y49 THEN "y49"
WHEN y2_y50 THEN "y50"
END AS LEAST_Y2,
CASE LEAST(
y3_y1,
y3_y2,
y3_y3,
y3_y4,
y3_y5,
y3_y6,
y3_y7,
y3_y8,
y3_y9,
y3_y10,
y3_y11,
y3_y12,
y3_y13,
y3_y14,
y3_y15,
y3_y16,
y3_y17,
y3_y18,
y3_y19,
y3_y20,
y3_y21,
y3_y22,
y3_y23,
y3_y24,
y3_y25,
y3_y26,
y3_y27,
y3_y28,
y3_y29,
y3_y30,
y3_y31,
y3_y32,
y3_y33,
y3_y34,
y3_y35,
y3_y36,
y3_y37,
y3_y38,
y3_y39,
y3_y40,
y3_y41,
y3_y42,
y3_y43,
y3_y44,
y3_y45,
y3_y46,
y3_y47,
y3_y48,
y3_y49,
y3_y50)
WHEN y3_y1 THEN "y1"
WHEN y3_y2 THEN "y2"
WHEN y3_y3 THEN "y3"
WHEN y3_y4 THEN "y4"
WHEN y3_y5 THEN "y5"
WHEN y3_y6 THEN "y6"
WHEN y3_y7 THEN "y7"
WHEN y3_y8 THEN "y8"
WHEN y3_y9 THEN "y9"
WHEN y3_y10 THEN "y10"
WHEN y3_y11 THEN "y11"
WHEN y3_y12 THEN "y12"
WHEN y3_y13 THEN "y13"
WHEN y3_y14 THEN "y14"
WHEN y3_y15 THEN "y15"
WHEN y3_y16 THEN "y16"
WHEN y3_y17 THEN "y17"
WHEN y3_y18 THEN "y18"
WHEN y3_y19 THEN "y19"
WHEN y3_y20 THEN "y20"
WHEN y3_y21 THEN "y21"
WHEN y3_y22 THEN "y22"
WHEN y3_y23 THEN "y23"
WHEN y3_y24 THEN "y24"
WHEN y3_y25 THEN "y25"
WHEN y3_y26 THEN "y26"
WHEN y3_y27 THEN "y27"
WHEN y3_y28 THEN "y28"
WHEN y3_y29 THEN "y29"
WHEN y3_y30 THEN "y30"
WHEN y3_y31 THEN "y31"
WHEN y3_y32 THEN "y32"
WHEN y3_y33 THEN "y33"
WHEN y3_y34 THEN "y34"
WHEN y3_y35 THEN "y35"
WHEN y3_y36 THEN "y36"
WHEN y3_y37 THEN "y37"
WHEN y3_y38 THEN "y38"
WHEN y3_y39 THEN "y39"
WHEN y3_y40 THEN "y40"
WHEN y3_y41 THEN "y41"
WHEN y3_y42 THEN "y42"
WHEN y3_y43 THEN "y43"
WHEN y3_y44 THEN "y44"
WHEN y3_y45 THEN "y45"
WHEN y3_y46 THEN "y46"
WHEN y3_y47 THEN "y47"
WHEN y3_y48 THEN "y48"
WHEN y3_y49 THEN "y49"
WHEN y3_y50 THEN "y50"
END AS LEAST_Y3,
CASE LEAST(
y4_y1,
y4_y2,
y4_y3,
y4_y4,
y4_y5,
y4_y6,
y4_y7,
y4_y8,
y4_y9,
y4_y10,
y4_y11,
y4_y12,
y4_y13,
y4_y14,
y4_y15,
y4_y16,
y4_y17,
y4_y18,
y4_y19,
y4_y20,
y4_y21,
y4_y22,
y4_y23,
y4_y24,
y4_y25,
y4_y26,
y4_y27,
y4_y28,
y4_y29,
y4_y30,
y4_y31,
y4_y32,
y4_y33,
y4_y34,
y4_y35,
y4_y36,
y4_y37,
y4_y38,
y4_y39,
y4_y40,
y4_y41,
y4_y42,
y4_y43,
y4_y44,
y4_y45,
y4_y46,
y4_y47,
y4_y48,
y4_y49,
y4_y50)
WHEN y4_y1 THEN "y1"
WHEN y4_y2 THEN "y2"
WHEN y4_y3 THEN "y3"
WHEN y4_y4 THEN "y4"
WHEN y4_y5 THEN "y5"
WHEN y4_y6 THEN "y6"
WHEN y4_y7 THEN "y7"
WHEN y4_y8 THEN "y8"
WHEN y4_y9 THEN "y9"
WHEN y4_y10 THEN "y10"
WHEN y4_y11 THEN "y11"
WHEN y4_y12 THEN "y12"
WHEN y4_y13 THEN "y13"
WHEN y4_y14 THEN "y14"
WHEN y4_y15 THEN "y15"
WHEN y4_y16 THEN "y16"
WHEN y4_y17 THEN "y17"
WHEN y4_y18 THEN "y18"
WHEN y4_y19 THEN "y19"
WHEN y4_y20 THEN "y20"
WHEN y4_y21 THEN "y21"
WHEN y4_y22 THEN "y22"
WHEN y4_y23 THEN "y23"
WHEN y4_y24 THEN "y24"
WHEN y4_y25 THEN "y25"
WHEN y4_y26 THEN "y26"
WHEN y4_y27 THEN "y27"
WHEN y4_y28 THEN "y28"
WHEN y4_y29 THEN "y29"
WHEN y4_y30 THEN "y30"
WHEN y4_y31 THEN "y31"
WHEN y4_y32 THEN "y32"
WHEN y4_y33 THEN "y33"
WHEN y4_y34 THEN "y34"
WHEN y4_y35 THEN "y35"
WHEN y4_y36 THEN "y36"
WHEN y4_y37 THEN "y37"
WHEN y4_y38 THEN "y38"
WHEN y4_y39 THEN "y39"
WHEN y4_y40 THEN "y40"
WHEN y4_y41 THEN "y41"
WHEN y4_y42 THEN "y42"
WHEN y4_y43 THEN "y43"
WHEN y4_y44 THEN "y44"
WHEN y4_y45 THEN "y45"
WHEN y4_y46 THEN "y46"
WHEN y4_y47 THEN "y47"
WHEN y4_y48 THEN "y48"
WHEN y4_y49 THEN "y49"
WHEN y4_y50 THEN "y50"
END AS LEAST_Y4
FROM DBSCHEMAARAS.quad_distanz
''')

result = cursor.fetchall()


cursor.close()
cursor = db.cursor()

#</Identifikation der Idealen Funktionen>------------------------------


#<Prüfung ob zugewiesene tatsächlich passend>--------------------------

#y1 -> y36
#y2 -> y11
#y3 -> y2
#y4 -> y33


#------------Train Y1-------------------

cursor.execute('''
SELECT train.x from DBSCHEMAARAS.train
''')

result = cursor.fetchall()

werte_x_float = result

cursor.close()
cursor = db.cursor()

cursor.execute('''
SELECT train.y1 from DBSCHEMAARAS.train
''')

result = cursor.fetchall()

train_y_float = result

plt.plot(werte_x_float, train_y_float)
plt.show()

cursor.execute('''
SELECT zuweisung_ideale.LEAST_y1 from DBSCHEMAARAS.zuweisung_ideale
''')

result = cursor.fetchall()

for row in result:
    ideal_for_y1_function = row[0]

cursor.close()
cursor = db.cursor()

cursor.execute( "SELECT ideal." + ideal_for_y1_function + " from DBSCHEMAARAS.ideal")

result = cursor.fetchall()

ideal_y_float = result

cursor.close()
cursor = db.cursor()


plt.plot(werte_x_float, ideal_y_float)
plt.show()

#------------Train Y2-------------------

cursor.execute('''
SELECT train.x from DBSCHEMAARAS.train
''')

result = cursor.fetchall()

werte_x_float = result

cursor.close()
cursor = db.cursor()

cursor.execute('''
SELECT train.y2 from DBSCHEMAARAS.train
''')

result = cursor.fetchall()

train_y_float = result

plt.plot(werte_x_float, train_y_float)
plt.show()

cursor.execute('''
SELECT zuweisung_ideale.LEAST_y2 from DBSCHEMAARAS.zuweisung_ideale
''')

result = cursor.fetchall()

for row in result:
    ideal_for_y2_function = row[0]

cursor.close()
cursor = db.cursor()

cursor.execute("SELECT ideal." + ideal_for_y2_function + " from DBSCHEMAARAS.ideal")

result = cursor.fetchall()

ideal_y_float = result

cursor.close()
cursor = db.cursor()


plt.plot(werte_x_float, ideal_y_float)
plt.show()
#------------Train Y3-------------------

cursor.execute('''
SELECT train.x from DBSCHEMAARAS.train
''')

result = cursor.fetchall()

werte_x_float = result

cursor.close()
cursor = db.cursor()

cursor.execute('''
SELECT train.y3 from DBSCHEMAARAS.train
''')

result = cursor.fetchall()

train_y_float = result

plt.plot(werte_x_float, train_y_float)
plt.show()

cursor.execute('''
SELECT zuweisung_ideale.LEAST_y3 from DBSCHEMAARAS.zuweisung_ideale
''')

result = cursor.fetchall()

for row in result:
    ideal_for_y3_function = row[0]

cursor.close()
cursor = db.cursor()

cursor.execute("SELECT ideal." + ideal_for_y3_function + " from DBSCHEMAARAS.ideal")

result = cursor.fetchall()

ideal_y_float = result

cursor.close()
cursor = db.cursor()


plt.plot(werte_x_float, ideal_y_float)
plt.show()
#------------Train Y4-------------------

cursor.execute('''
SELECT train.x from DBSCHEMAARAS.train
''')

result = cursor.fetchall()

werte_x_float = result

cursor.close()
cursor = db.cursor()

cursor.execute('''
SELECT train.y4 from DBSCHEMAARAS.train
''')

result = cursor.fetchall()

train_y_float = result

plt.plot(werte_x_float, train_y_float)
plt.show()

cursor.execute('''
SELECT zuweisung_ideale.LEAST_y4 from DBSCHEMAARAS.zuweisung_ideale
''')

result = cursor.fetchall()

for row in result:
    ideal_for_y4_function = row[0]

cursor.close()
cursor = db.cursor()

cursor.execute("SELECT ideal." + ideal_for_y4_function + " from DBSCHEMAARAS.ideal")

result = cursor.fetchall()

ideal_y_float = result

cursor.close()
cursor = db.cursor()


plt.plot(werte_x_float, ideal_y_float)
plt.show()

#</Prüfung ob zugewiesene tatsächlich passend>--------------------------

#<Identifikation M für unsere 4 idealen Funktionen>---------------------
# M ist die maximale Abweichung zwischen Datensatz idealer Funktion und Testdatensatz

cursor.execute('''
CREATE TABLE distanzIdealTestFuerM AS SELECT t.x as "x-Test", t.y as "y-Test",
t.y - i.''' + ideal_for_y1_function + ''' as "distanz_Zu_y1",
t.y - i.''' + ideal_for_y2_function + ''' as "distanz_Zu_y2",
t.y - i.''' + ideal_for_y3_function + ''' as "distanz_Zu_y3",
t.y - i.''' + ideal_for_y4_function + ''' as "distanz_Zu_y4"
FROM DBSCHEMAARAS.ideal as i
LEFT OUTER JOIN DBSCHEMAARAS.test as t on t.x = i.x
''')

result = cursor.fetchall()


cursor.close()
cursor = db.cursor()

cursor.execute('''
DELETE FROM DBSCHEMAARAS.distanzIdealTestFuerM 
WHERE distanz_Zu_y1 IS NULL and distanz_Zu_y2 IS NULL and distanz_Zu_y3 IS NULL and distanz_Zu_y4 IS NULL
''')

result = cursor.fetchall()


cursor.close()
cursor = db.cursor()

# cursor.execute('''
# CREATE TABLE quad_distanzIdealTestfuerM AS SELECT
# POWER((y1_y),2) as "y1_y",
# POWER((y2_y),2) as "y2_y",
# POWER((y3_y),2) as "y3_y",
# POWER((y4_y),2) as "y4_y"
# FROM DBSCHEMAARAS.distanzIdealTestfuerM
# ''')
#
# result = cursor.fetchall()
#
#
# cursor.close()
# cursor = db.cursor()

# cursor.execute('''
# CREATE TABLE max_distanzIdealTestfuerM AS SELECT
# MAX(y1_y) as "M_y1",
# MAX(y2_y) as "M_y2",
# MAX(y3_y) as "M_y3",
# MAX(y4_y) as "M_y4"
# FROM DBSCHEMAARAS.distanzIdealTestfuerM
# ''')
#
# result = cursor.fetchall()
#
#
# cursor.close()
# cursor = db.cursor()


#</Identifikation M>----------------------------------------------------

#<Identifikation N>-----------------------------------------------------


cursor.execute('''
CREATE TABLE bestimmung_N AS SELECT CASE GREATEST(
y1_''' + ideal_for_y1_function + ''',
y2_''' + ideal_for_y2_function + ''',
y3_''' + ideal_for_y3_function + ''',
y4_''' + ideal_for_y4_function + ''')
WHEN y1_''' + ideal_for_y1_function + ''' THEN y1_''' + ideal_for_y1_function + '''
WHEN y2_''' + ideal_for_y2_function + ''' THEN y2_''' + ideal_for_y2_function + '''
WHEN y3_''' + ideal_for_y3_function + ''' THEN y3_''' + ideal_for_y3_function + '''
WHEN y4_''' + ideal_for_y4_function + ''' THEN y4_''' + ideal_for_y4_function + '''
END AS N
FROM DBSCHEMAARAS.quad_distanz
''')

result = cursor.fetchall()


cursor.close()
cursor = db.cursor()

#</Identifikation N>----------------------------------------------------

#<Berechnung der M/N Formel>--------------------------------------------
# M < (sqrt(2))*N ?

# fetching M
cursor.execute(''' SELECT x-Test, y-Test, distanz_Zu_y1, distanz_Zu_y2, distanz_Zu_y3, distanz_Zu_y4
FROM DBSCHEMAARAS.distanzIdealTestFuerM''')

result_m = cursor.fetchall()
cursor.close()
cursor = db.cursor()

#Fetching N

cursor.execute('''
SELECT N FROM BSCHEMAARAS.bestimmung_N
''')

result_n = cursor.fetchall()
cursor.close()
cursor = db.cursor()

for n in result_n:
    nWert = n[0]

listy1 = []
listy1Tuple = []
listy2 = []
listy2Tuple = []
listy3 = []
listy3Tuple = []
listy4 = []
listy4Tuple = []

# hier kommen die Testwerte und ihre Distanz zur idealfunktion rein die M < sqrt(2) * N erfüllen:

cursor.execute('''
CREATE TABLE distanzIdealTestFormelFiltered (
    x-Test type decimal(21,12),
    y-Test type decimal(21,12),
    distanz type decimal(21,12),
    ideal type decimal(21,12) )
''')

result = cursor.fetchall()


cursor.close()
cursor = db.cursor()


for m in result_m:
    idealy1Toleranz = m[2] < (math.sqrt(2) * nWert)
    if idealy1Toleranz is True:
            listy1Tuple.append(m[0])
            listy1Tuple.append(m[1])
            listy1Tuple.append(m[2])
            listy1Tuple.append('y1')
#            listy1.append(listy1Tuple)
            statement = 'INSERT INTO dbschemaaras.distanzIdealTestFormelFiltered (x-Test, y-Test, distanz, ideal) VALUES (%s,%s,%s,%s)'
            cursor.execute(statement, listy4Tuple)
            db.commit()

            result = cursor.fetchall()
            cursor.close()
            cursor = db.cursor()


    idealy2Toleranz = m[3] < (math.sqrt(2) * nWert)
    if idealy2Toleranz is True:
            listy2Tuple.append(m[0])
            listy2Tuple.append(m[1])
            listy2Tuple.append(m[3])
            listy2Tuple.append('y2')
#            listy2.append(listy2Tuple)
            statement = 'INSERT INTO dbschemaaras.distanzIdealTestFormelFiltered (x-Test, y-Test, distanz, ideal) VALUES (%s,%s,%s,%s)'
            cursor.execute(statement, listy4Tuple)
            db.commit()

            result = cursor.fetchall()
            cursor.close()
            cursor = db.cursor()

    idealy3Toleranz = m[4] < (math.sqrt(2) * nWert)
    if idealy3Toleranz is True:
            listy3Tuple.append(m[0])
            listy3Tuple.append(m[1])
            listy3Tuple.append(m[4])
            listy3Tuple.append('y3')
#            listy3.append(listy3Tuple)
            statement = 'INSERT INTO dbschemaaras.distanzIdealTestFormelFiltered (x-Test, y-Test, distanz, ideal) VALUES (%s,%s,%s,%s)'
            cursor.execute(statement, listy3Tuple)
            db.commit()

            result = cursor.fetchall()
            cursor.close()
            cursor = db.cursor()


    idealy4Toleranz = m[5] < (math.sqrt(2) * nWert)
    if idealy4Toleranz is True:
            listy4Tuple.append(m[0])
            listy4Tuple.append(m[1])
            listy4Tuple.append(m[5])
            listy4Tuple.append('y4')
#            listy4.append(listy4Tuple)
            statement = 'INSERT INTO dbschemaaras.distanzIdealTestFormelFiltered (x-Test, y-Test, distanz, ideal) VALUES (%s,%s,%s,%s)'
            cursor.execute(statement, listy4Tuple)
            db.commit()

            result = cursor.fetchall()
            cursor.close()
            cursor = db.cursor()





#</Berechnung der M/N Formel>-------------------------------------------

#<Löschen der Tabellen ideal, test und train zur ermöglichung der Neuausführung>

cursor.execute('''
DROP TABLE DBSCHEMAARAS.ideal
''')

result = cursor.fetchall()
cursor.close()
cursor = db.cursor()


cursor.execute('''
DROP TABLE DBSCHEMAARAS.test
''')

result = cursor.fetchall()
cursor.close()
cursor = db.cursor()

cursor.execute('''
DROP TABLE DBSCHEMAARAS.train
''')

result = cursor.fetchall()
cursor.close()
cursor = db.cursor()

cursor.execute('''
DROP TABLE DBSCHEMAARAS.quad_distanz
''')

result = cursor.fetchall()
cursor.close()
cursor = db.cursor()

cursor.execute('''
DROP TABLE DBSCHEMAARAS.zuweisung_ideale
''')

result = cursor.fetchall()
cursor.close()
cursor = db.cursor()


#<Löschen der Tabellen ideal, test und train zur ermöglichung der Neuausführung>
