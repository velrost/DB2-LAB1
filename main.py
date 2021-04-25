import psycopg2
import psycopg2.errorcodes
import csv
import datetime
import itertools

# Connect to db
conn = psycopg2.connect(dbname='postgres', user='postgres',
                        password='admin', host='localhost', port="5432")
cursor = conn.cursor()
# Drop table if exists
cursor.execute('DROP TABLE IF EXISTS TABLE1;')


# Create table
def create_table():

    with open("Odata2019File.csv", "r", encoding="cp1251") as csv_file:
        # Columns name
        columns = csv_file.readline().split(';')
        columns = [word.strip('"') for word in columns]
        columns[-1] = columns[-1].rstrip('"\n')
        # Creating table columns
        clm_names = "\n\tYear INT,"
        for elm in columns:
            if elm == 'OUTID':
                clm_names += '\n\t' + elm + ' VARCHAR(40),'
            elif elm == 'Birth':
                clm_names += '\n\t' + elm + ' INT,'
            elif 'Ball' in elm:
                clm_names += '\n\t' + elm + ' INT,'
            else:
                clm_names += '\n\t' + elm + ' VARCHAR(255),'
        query = '''CREATE TABLE IF NOT EXISTS TABLE1(''' + clm_names.rstrip(',') + \
                '\n, PRIMARY KEY(OUTID)\n)'
        cursor.execute(query)
        conn.commit()
        return columns


columns = create_table()

print("\nTable was created\n")

print("--------------------------\n")
print("--------------------------\n")

conn.commit()

# Import data to table with boot time measurement з виміром часу завантаження
# Create update if connection was lost
def insert_from_csv(data, year, conn, cursor, logs_f):

    start_time = datetime.datetime.now()
    logs_f.write(str(start_time) + " - open file time " +data+ '\n')

    with open(data, "r", encoding="cp1251") as csv_file:
        # Start reading csv data
        print("Reading file " + data + ' ...')
        print("--------------------------\n")
        print("--------------------------\n")
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        batches_inserted = 0
        batch_size = 100
        inserted_all = False

        while not inserted_all:
            try:
                insert_query = '''INSERT INTO TABLE1 (year, ''' + ', '.join(columns) + ') VALUES '
                count = 0
                for row in csv_reader:
                    count += 1
                    # Take text values in marks and change , to .
                    for key in row:
                        if row[key] == 'null':
                            pass
                        elif key.lower() != 'birth' and 'ball' not in key.lower():
                            row[key] = "'" + row[key].replace("'", "''") + "'"
                        elif 'ball100' in key.lower():
                            row[key] = row[key].replace(',', '.')
                    insert_query += '\n\t(' + str(year) + ', ' + ','.join(row.values()) + '),'

                    # If 100 lines inserted
                    if count == batch_size:
                        count = 0
                        insert_query = insert_query.rstrip(',') + ';'
                        cursor.execute(insert_query)
                        conn.commit()
                        batches_inserted += 1
                        insert_query = '''INSERT INTO TABLE1 (year, ''' + ', '.join(columns) + ') VALUES '

                # If csv file ended
                if count != 0:
                    insert_query = insert_query.rstrip(',') + ';'
                    cursor.execute(insert_query)
                    conn.commit()
                inserted_all = True


            # Case if connecstion was lost
            except psycopg2.OperationalError:
                if psycopg2.OperationalError.pgcode == psycopg2.errorcodes.ADMIN_SHUTDOWN:
                    print("Connection was lost")
                    print("--------------------------\n")
                    print("--------------------------\n")
                    connection_restored = False
                    while not connection_restored:
                        # Restorring connection
                        try:
                            connection = psycopg2.connect(host="localhost",
                                                          database="postgres",
                                                          user="postgres",
                                                          password="admin",
                                                          port="5432")
                            cursor = connection.cursor()
                            connection_restored = True
                        except psycopg2.OperationalError:
                            pass
                    print("Connection was restored")
                    print("--------------------------\n")
                    print("--------------------------\n")

    end_time = datetime.datetime.now()
    logs_f.write(str(end_time) + " - all data was inserted\n")
    logs_f.write('Time for inserting data - ' + str(end_time - start_time) + '\n\n')

    return conn, cursor

# Creating file with time
logs_file = open('log_of_time.txt', 'w')
conn, cursor = insert_from_csv("Odata2019File.csv", 2019, conn, cursor, logs_file)
conn, cursor = insert_from_csv("Odata2020File.csv", 2020, conn, cursor, logs_file)
logs_file.close()

print("Both files inserted into csv\n")
print("--------------------------\n")
print("--------------------------\n")

# Query function
def query_result():
    query1 = '''
        SELECT REGNAME, YEAR, min(mathBall100)
        FROM TABLE1
        WHERE mathTestStatus = 'Зараховано'
        GROUP BY REGNAME, YEAR;
        '''
    cursor.execute(query1)
    print("Select created")
    print("--------------------------\n")
    print("--------------------------\n")

    with open('result.csv', 'w', encoding="utf-8") as data:
        csv_writer = csv.writer(data)
        csv_writer.writerow(['Область', 'Рік', 'Найгірший бал з Математики'])
        for row in cursor:
            csv_writer.writerow(row)

print("Result of SELECT inserted into 'result.csv' ")
print("--------------------------\n")
print("--------------------------\n")

query_result()

cursor.close()
conn.close()