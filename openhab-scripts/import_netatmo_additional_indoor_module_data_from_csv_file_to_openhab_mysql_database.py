import glob, csv, datetime, time, sys, mysql.connector, timeit

#
# Configuration
#

# Database
dbconfig = {
        'host' : '127.0.0.1',
        'port' : '3306',
        'user' : 'userid',
        'password' : 'password',
        'database' : 'openhab'
    }

dbtabletemperature = "Item8"
dbtablehumidity = "Item6"
dbtableco2 = "Item11"

# CSV files to import
csvdirectory = "/Users/openhab/Downloads/"
csvfilepattern = "NetatmoAdditionalModuleName*.csv"
csvencoding = "utf-8"
csvdelimiter = ";"

#
# End of configuration
#

# Connect to database
print()
try:
    connection = mysql.connector.connect(**dbconfig)
    print ("Database connected.")
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("User and/or password mismatch.")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist.")
  else:
    print(err)
    print("No connection to database with configuration: ", **dbconfig)
  sys.exit(0)

cursor = connection.cursor()

# Iterate over csv files
csvpattern = csvdirectory + csvfilepattern 
listfiles = glob.glob(csvpattern)
allrows = 0
tempduplicate = 0
tempwritten = 0
humidduplicate = 0
humidwritten = 0
co2duplicate = 0
co2written = 0

tic=timeit.default_timer()

for csvfile in listfiles:
    print("Processing ", csvfile, "...")
    ifile = open(csvfile, encoding=csvencoding)
    reader = csv.reader(ifile, delimiter=csvdelimiter)
    rowcount = 0
    for row in reader:
        rowcount += 1
        # Exported Netatmo data starts at row 5
        if rowcount >= 5:
            allrows += 1
            # date = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(row[1], "%Y/%m/%d %H:%M:%S"))
            # Replace "/" with "-"
            date = row[1].replace("/","-")
    
            temperature = row[2]
            humidity = row[3]
            co2 = row[4]
    
            # Check for existing temperature values
            sqlcommand = "SELECT * FROM " + dbtabletemperature + " WHERE Time = '" + date + "'"
            cursor.execute(sqlcommand)
            result = cursor.fetchall()
            # Duplicate entry
            if result:
                tempduplicate += 1
            # New entry
            else:
                sqltemp="INSERT INTO " + dbtabletemperature + " (Time,Value) VALUES ('" + date +"','" + temperature +"')"
                try:
                    cursor.execute(sqltemp)
                    tempwritten += 1
                except:
                    print("Error INSERTing temperature data:",sqltemp)
                    sys.exit(0)
    
            # Check for existing humidity values
            sqlcommand = "SELECT * FROM " + dbtablehumidity + " WHERE Time = '" + date + "'"
            cursor.execute(sqlcommand)
            result = cursor.fetchall()
            if result:
                humidduplicate += 1
            else:
                sqlhumid="INSERT INTO " + dbtablehumidity + " (Time,Value) VALUES ('" + date +"','" + humidity +"')"
                try:
                    cursor.execute(sqlhumid)
                    humidwritten += 1
                except:
                    print("Error INSERTing humidity data:",sqlhumid)
                    sys.exit(0)

            # Check for existing CO2 values
            sqlcommand = "SELECT * FROM " + dbtableco2 + " WHERE Time = '" + date + "'"
            cursor.execute(sqlcommand)
            result = cursor.fetchall()
            if result:
                co2duplicate += 1
            else:
                sqlco2="INSERT INTO " + dbtableco2 + " (Time,Value) VALUES ('" + date +"','" + co2 +"')"
                try:
                    cursor.execute(sqlco2)
                    co2written += 1
                except:
                    print("Error INSERTing CO2 data:",sqlco2)
                    sys.exit(0)

    connection.commit ()
    ifile.close()

toc=timeit.default_timer()
# Close database connection
cursor.close()
connection.close()
print("Database disconnected.")

# Summary
print()
print("Elapsed time (in seconds):", round(toc-tic,2))
print("Total rows:", allrows )
print("Total rows per second:", round (allrows / (toc-tic),2))
print("Skipped temperature entries: ", tempduplicate)
print("Number of temperatures entries written: ", tempwritten)
print("Skipped humidity entries: ", humidduplicate)
print("Number of humidity entries written: ", humidwritten)
print("Skipped CO2 entries: ", co2duplicate)
print("Number of CO2 entries written: ", co2written)
print()
