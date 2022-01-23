import pandas as pd
import pyodbc


path = '/Users/mikailaras/Desktop/CSV/'
datei_ideal = 'ideal.csv'
datei_train = 'train.csv'
datei_test = 'test.csv'

data = pd.read_csv (r'/Users/mikailaras/Desktop/CSV/train.csv')
df = pd.DataFrame(data)

print(df)


conn = pyodbc.connect('Driver={MySQL ODBC 3.51 Driver};'
                      'Server=localhost;'
                      'Database=dbschemaaras;'
                      'Trusted_Connection=yes;'
                      'user=root'
                      'password=Lo34pg8g/new'
                      'OPTION =3')
cursor = conn.cursor()

cursor.execute('''
		CREATE TABLE ideal (
			x int primary key,
			y1 DECIMAL(16,12),
			y2 DECIMAL(16,12),
			y3 DECIMAL(16,12),
			y4 DECIMAL(16,12),
			)
               ''')