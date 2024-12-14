from sqlalchemy import Float, Table, Column, Integer, String, ForeignKey, MetaData, Date
import sqlalchemy
from sqlalchemy import create_engine, text, inspect, Table, Column, Integer, String, MetaData
from sqlalchemy.engine import Inspector

# Define metadata
metadata = MetaData()

# Define tables with keys
clean_stations = Table(
    'clean_stations', metadata,
    Column('station', String, primary_key=True),  # Assuming 'station_id' is the primary key
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('Country', String),
    Column('State', String),
)

clean_measure = Table(
    'clean_measure', metadata,
    Column('station', String, ForeignKey('clean_stations.station')),
    Column('date', Date),  
    Column('precip', Float),
    Column('tobs', Integer), 
)

# Create a SQLAlchemy engine (SQLite example)
engine = create_engine('sqlite:///zadanie6_3.db')  

# Create tables in the database
metadata.create_all(engine)


import pandas as pd

# Load the CSV file
df_clean_stations = pd.read_csv('C:/Kodilla/Modul_6/work/clean_stations.csv')  
df_clean_measure = pd.read_csv('C:/Kodilla/Modul_6/work/clean_measure.csv')  


# Write the DataFrame to a SQL table
table_clean_stations = 'clean_stations'  
df_clean_stations.to_sql(table_clean_stations, con=engine, if_exists='replace', index=False)


table_clean_measure = 'clean_measure'
df_clean_measure.to_sql(table_clean_measure, con=engine, if_exists='replace', index=False)


inspector = inspect(engine)
print(f"To sa nazwy zaimportowanych tabeli: {inspector.get_table_names()}")


# Example of selection/edition of the db (see below)

# Select stations with latitude in a certain range
latitude_min = 21.4  # Example minimum latitude
latitude_max = 21.5  # Example maximum latitude

with engine.connect() as connection:
    query_select = text("""
        SELECT * FROM clean_stations
        WHERE latitude BETWEEN :lat_min AND :lat_max
    """)
    result = connection.execute(query_select, {"lat_min": latitude_min, "lat_max": latitude_max})


# Uncomment to see update and delete on the database
'''
with engine.connect() as connection:
    query_update = text("""
        UPDATE clean_stations
        SET name = 'Updated row'
        WHERE latitude BETWEEN :lat_min AND :lat_max
    """)
    connection.execute(query_update, {"lat_min": latitude_min, "lat_max": latitude_max})
    connection.commit() 

with engine.connect() as connection:
    query_delete = text("""
        DELETE FROM clean_stations WHERE latitude BETWEEN :lat_min AND :lat_max
    """)
    connection.execute(query_delete, {"lat_min": latitude_min, "lat_max": latitude_max})
    connection.commit()

with engine.connect() as connection:
    result = connection.execute(text("SELECT * FROM clean_stations LIMIT 5"))  
    rows = result.fetchall()  # Fetch all rows
    print(rows)  # Print the fetched rows
'''