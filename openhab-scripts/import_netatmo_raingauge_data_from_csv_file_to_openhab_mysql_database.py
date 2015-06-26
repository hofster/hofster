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
dbtablerain = "Item13"

# CSV files to import
csvdirectory = "/Users/openhab/Downloads/"
csvfilepattern = "NetatmoRainGauge*.csv"
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
rainduplicate = 0
rainwritten = 0
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
            rain = row[2]
    
            # Check for existing temperature values
            sqlcommand = "SELECT * FROM " + dbtablerain + " WHERE Time = '" + date + "'"
            cursor.execute(sqlcommand)
            result = cursor.fetchall()
            # Duplicate entry
            if result:
                rainduplicate += 1
            # New entry
            else:
                sqltemp="INSERT INTO " + dbtablerain + " (Time,Value) VALUES ('" + date +"','" + rain +"')"
                try:
                    cursor.execute(sqltemp)
                    rainwritten += 1
                except:
                    print("Error INSERTing rain data:",sqltemp)
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
print ("Skipped rain entries: ", rainduplicate)
print ("Number of rain entries written: ", rainwritten)
print()
