import sqlite3

# Create tables in database
conn = sqlite3.connect("C:\\Users\\nochum.paltiel\Documents\PycharmProjects\\recruitee_data_analysis\\recruitee.db")
curs = conn.cursor()

# Create all tables


curs.close()
conn.close()