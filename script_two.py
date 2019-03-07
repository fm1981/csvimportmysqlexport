import os
import re
import sys
import csv
import collections
import warnings
import mysql.connector

mydb = mysql.connector.connect(user='fmorgan1981', passwd='batman66', database='fraser_test', host='85.10.205.173', port = '3306')
print("\nConnection to DB established\n")
mycursor = mydb.cursor()
mycursor.execute("SHOW columns FROM animal")
print([column[0] for column in mycursor.fetchall()])

animal_average = mycursor.execute("SELECT AVG(height) AS average FROM animal")

result = mycursor.fetchall()

for i in result:
    print(i[0])

mycursor.close()
mydb.close()
