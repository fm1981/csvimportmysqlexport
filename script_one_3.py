import os
import re
import sys
import csv
import collections
import warnings
import mysql.connector


def find_type(s):
    #Find type for this string
    # try integer type

    try:
        v = int(s)
    except ValueError:
        pass
    else:
        if abs(v) > 2147483647:
            return 'bigint'
        else:
            return 'int'

    # try float type
    try:
        float(s)
    except ValueError:
        pass
    else:
        return 'double'


    # doesn't match any other types so assume text
    if len(s) > 255:
        return 'text'
    else:
        return 'varchar(255)'

def find_common(l):
        return max(l, key=l.count)


def find_col_types(input_file, max_rows=1000):
    #Find the type for each CSV column

    csv_types = collections.defaultdict(list)
    reader = csv.reader(open(input_file))
    #i = 0

    # test the first few rows for their data types
    for row_counter, row_contents in enumerate(reader):
        if row_counter == 0:
            header = row_contents
            print(header)
        else:
            for col_counter, s in enumerate(row_contents):
                data_type = find_type(s)
                csv_types[header[col_counter]].append(data_type)
                #print(csv_types)
                #i = i+1
                #print(i)

        if row_counter == max_rows:
            break

    # take the most common data type for each row
    return [find_common(csv_types[col]) for col in header]

def gen_schema(table, header, col_types):
    #Generate the schema for this table
    schema_sql = """CREATE TABLE IF NOT EXISTS %s (
        id int NOT NULL AUTO_INCREMENT,""" % table

    for col_name, col_type in zip(header, col_types):
        schema_sql += '\n%s %s,' % (col_name, col_type)

    schema_sql += """\nPRIMARY KEY (id)
        ) DEFAULT CHARSET=utf8;"""
    return schema_sql

def gen_insert(table, header):
    #Generate the SQL for inserting rows

    field_names = ', '.join(header)
    field_markers = ', '.join('%s' for col in header)
    return 'INSERT INTO %s (%s) VALUES (%s);' % \
        (table, field_names, field_markers)

#def format_header(row):
    #Format column names to remove illegal characters and duplicates

#    safe_col = lambda s: re.sub('\W+', '_', s.lower()).strip('_')
#    header = []
#    counts = collections.defaultdict(int)
#    for col in row:
#        col = safe_col(col)
#        counts[col] += 1
#        if counts[col] > 1:
#            col = '{}{}'.format(col, counts[col])
#        header.append(col)
#    return header


mydb = mysql.connector.connect(user='fmorgan', passwd='U#qUC76d', database='quantix_test', host='85.10.205.173', port = '3306')
print("\nConnection to DB established\n")
mycursor = mydb.cursor()
csvfile = 'tabledata.csv'
print('Analyzing column types ...')
col_types = find_col_types(csvfile)
print(col_types)
table = 'table1'

header = None
with open(csvfile) as f:
    reader = csv.reader(f)
    for row in reader:
        if header:
            mycursor.execute(insert_sql, row)
        else:
            header = (row)
            n = len(header)
            for col in range(n):
              header[col] = (header[col].encode('ascii', 'ignore')).decode("utf-8")

            schema_sql = gen_schema(table, header, col_types)
            print("This is the schema:\n")
            print(schema_sql)
            # create table
            mycursor.execute(schema_sql)
            print('Inserting rows ...')
            # SQL string for inserting data
            insert_sql = gen_insert(table, header)


# commit rows to database
print('Committing rows to database ...')
mydb.commit()
mycursor.close()
mydb.close()
print('Done!')
