# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import csv
from matplotlib import pyplot as plt

#Pfad unserer CSV Dateien
path = '/Users/mikailaras/Desktop/CSV/'

#Listen zur Persistierung der X-Achsen- und Y-Achsen-Daten
ideal_x = []
ideal_y = []

test_x = []
test_y = []

train_x = []
train_y = []


with open( path + 'ideal.csv', newline='') as csvfile:
    ideal = csv.reader(csvfile, delimiter=',', quotechar='|')
    counter = 0

    # X & Y Achsen Beladung
    for row in ideal:
        if counter != 0:
            ideal_x.append(row[0])
            ideal_y.append(row[1])
        counter = counter + 1


ideal_x_float = [float(element) for element in ideal_x]
ideal_y_float = [float(element) for element in ideal_y]

with open( path + 'test.csv', newline='') as csvfile:
    test = csv.reader(csvfile, delimiter=',', quotechar='|')
    counter = 0

    # X Achsen Beladung
    for row in test:
        if counter != 0:
            test_x.append(row[0])
        counter = counter + 1

with open( path + 'train.csv', newline='') as csvfile:
    train = csv.reader(csvfile, delimiter=',', quotechar='|')
    counter = 0

    # X Achsen Beladung
    for row in train:
        if counter != 0:
            train_x.append(row[0])
        counter = counter + 1


print(ideal_x)
print(ideal_y)


plt.plot(ideal_x_float, ideal_y_float)
plt.show()