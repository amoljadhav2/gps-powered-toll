import psycopg2
import time
from math import radians, sin, cos, sqrt, atan2

last_processed_timestamp = None  # Initialize with None or a specific timestamp
last_match_time = time.time()  # Initialize last match time
toll_point_matches = {}  # Dictionary to store the number of matches for each toll point ID

# Function to calculate distance between two points using Haversine formula
def calculate_distance(lat1, lon1, lat2, lon2):
    # Radius of the Earth in meters
    R = 6371000
    # Convert latitude and longitude from degrees to radians
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)
    # Calculate differences in latitude and longitude
    d_lat = lat2_rad - lat1_rad
    d_lon = lon2_rad - lon1_rad
    # Calculate distance using Haversine formula
    a = sin(d_lat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(d_lon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance

# Function to connect to the database
def connect_to_database():
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="semicolon24",
            host="postgres.cbqwc6simkya.us-east-1.rds.amazonaws.com",
            port="5432",
            database="postgres"
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
        return None

# Function to fetch new records from the vehicle_location table
def fetch_new_vehicle_locations(connection):
    global last_processed_timestamp
    try:
        cursor = connection.cursor()
        if last_processed_timestamp:
            cursor.execute("SELECT * FROM vehicle_location WHERE updt_dt > %s", (last_processed_timestamp,))
        else:
            cursor.execute("SELECT * FROM vehicle_location")
        new_records = cursor.fetchall()
        # Update last_processed_timestamp if new_records
        if new_records:
            last_processed_timestamp = max(record[4] for record in new_records)  # Assuming updt_date is at index 4
        return new_records
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching new records:", error)
        return []

# Function to fetch toll points from the database
def fetch_toll_points(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM toll_point")
        toll_points = cursor.fetchall()
        return toll_points
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching toll points:", error)
        return []

# Function to insert into toll_point_cross table
def insert_into_toll_point_cross(connection, vehicle_reg_num, toll_point_id):
    try:
        cursor = connection.cursor()
        cursor.execute("INSERT INTO toll_point_cross (toll_point_cross_id, vehicle_reg_num, direction) VALUES (%s, %s, %s)", (toll_point_id, vehicle_reg_num, 'towards infosys circle'))
        connection.commit()
        print(f"Inserted record for vehicle with registration number {vehicle_reg_num} near toll point with ID {toll_point_id}")
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting record into toll_point_cross:", error)

# Function to check if a vehicle is near a toll point and insert into toll_point_cross table
def check_and_insert_toll_point_cross(record, connection, toll_points):
    global last_match_time, toll_point_matches
    vehicle_latitude = record[2]  # Index 2 corresponds to veh_latitude in the record
    vehicle_longitude = record[3]  # Index 3 corresponds to veh_longitude in the record
    for toll_point in toll_points:
        distance = calculate_distance(vehicle_latitude, vehicle_longitude, toll_point[2], toll_point[3])  # Assuming toll_point is a tuple with latitude at index 2 and longitude at index 3
        if distance < 50:  # Check if vehicle is within 50 meters of toll point
            # Update last match time
            last_match_time = time.time()
            # Update number of matches for toll point ID
            toll_point_id = toll_point[1]  # Assuming toll_point_id is at index 1
            toll_point_matches[toll_point_id] = toll_point_matches.get(toll_point_id, 0) + 1
            # Check if any toll point has reached 5 matches
            if toll_point_matches[toll_point_id] >= 5:
                return True  # End processing
            insert_into_toll_point_cross(connection, record[1], toll_point_id)  # Index 1 corresponds to toll_point_id
            # Check if 5 minutes have elapsed since the last match
            if time.time() - last_match_time >= 300:  # 300 seconds = 5 minutes
                return True  # End processing
    return False  # Continue processing

# Function to calculate total distance and toll amount
def calculate_total_distance_and_toll(connection):
    try:
        cursor = connection.cursor()
        # Get minimum and maximum toll_point_cross_id from toll_point_cross table
        cursor.execute("SELECT MIN(toll_point_cross_id), MAX(toll_point_cross_id) FROM toll_point_cross")
        min_max_ids = cursor.fetchone()
        if min_max_ids:
            min_id, max_id = min_max_ids
            # Calculate difference between min and max toll_point_cross_id
            result = max_id - min_id
            # Calculate total distance
            total_distance = result * 200  # Assuming each unit is 200 meters
            # Fetch toll fare per meter from toll fare table
            cursor.execute("SELECT toll_amt_per_meter FROM toll_fare")
            toll_fare_per_meter = cursor.fetchone()[0]
            # Calculate total toll
            total_toll = total_distance * toll_fare_per_meter
            # Fetch vehicle_reg_num and direction from toll_point_cross table
            cursor.execute("SELECT vehicle_reg_num, direction FROM toll_point_cross WHERE toll_point_cross_id = %s", (min_id,))
            reg_num, direction = cursor.fetchone()
            # Insert total distance, total toll, vehicle_reg_num, and direction into new table
            cursor.execute("INSERT INTO user_toll_calculation (vehicle_reg_num, total_distance, total_toll, direction) VALUES (%s, %s, %s, %s)", (reg_num, total_distance, total_toll, direction))
            connection.commit()
            print("Total distance, total toll, vehicle registration number, and direction inserted into database.")
        else:
            print("Error: Unable to fetch minimum and maximum toll_point_cross_id.")
    except (Exception, psycopg2.Error) as error:
        print("Error while calculating total distance and total toll:", error)

def main():
    connection = connect_to_database()
    if connection:
        toll_points = fetch_toll_points(connection)
        while True:
            new_records = fetch_new_vehicle_locations(connection)
            if new_records:
                for record in new_records:
                    end_processing = check_and_insert_toll_point_cross(record, connection, toll_points)
                    if end_processing:
                        calculate_total_distance_and_toll(connection)
                        return  # End the main loop and exit
            time.sleep(3)  # Adjust the interval as needed (e.g., 3 seconds)
    else:
        print("Failed to establish database connection. Exiting.")

if __name__ == "__main__":
    main()

