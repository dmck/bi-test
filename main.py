import pyodbc
import pandas as pd
import pydantic
import random
import datetime
import time
import multiprocessing
from config import server, database, username, password

# columns in the parking-garage table
# columns = ['plate', 'entry_time', 'exit_time']

# create car class
class Car(pydantic.BaseModel):
    plate: str
    entry_time: str
    exit_time: str | None

# create a connection to the database
def create_connection():
    conn = pyodbc.connect(f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}')
    return conn

# add a car to the database
def add_car_to_database(car, conn, cursor):
    try:
        # insert the car into the database
        cursor.execute(f"INSERT INTO dbo.ParkingGarage (plate, entry_time) VALUES ('{car.plate}', '{car.entry_time}')")
        conn.commit()

        # print the car
        print(f'Added car: {car}')
    # generic exception to catch any error
    except Exception as e:
        print(f'Error adding car: {e}')

def exit_car_from_database(car, conn, cursor):
    try:
        # update the car in the database
        cursor.execute(f"UPDATE dbo.ParkingGarage SET exit_time = '{car.exit_time}' WHERE plate = '{car.plate}'")
        # get the row
        conn.commit()

        # print the car
        print(f'Exited car: {car}')
    except Exception as e:
        print(f'Error exiting car: {e}')

def create_cars(q):
    # create a connection to the database
    conn = create_connection()
    cursor = conn.cursor()

    for i in range(10):
        # create a random car plate 6 characters long
        plate = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890', k=6))
        entry_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        exit_time = None

        # create a car object, make the times strings
        car = Car(plate=plate, entry_time=entry_time, exit_time=exit_time)

        # add the car to the database
        add_car_to_database(car, conn, cursor)

        # add the car to the queue
        q.put(car)

        # sleep for a random amount of time about 5 seconds give or take 2
        time.sleep(random.randint(3, 7))

    # close the connection
    conn.close()

def exit_cars():
    # create a connection to the database
    conn = create_connection()
    cursor = conn.cursor()

    cars = []
    # get all cars from the database with no exit time
    cursor.execute("SELECT * FROM dbo.ParkingGarage WHERE exit_time IS NULL")
    rows = cursor.fetchall()

    for row in rows:
        car = Car(plate=row.plate, entry_time=row.entry_time.strftime('%Y-%m-%d %H:%M:%S'), exit_time=row.exit_time)
        cars.append(car)

    while len(cars) > 0:
        # pick a random car and have it exit
        car = random.choice(cars)
        car.exit_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # exit the car from the database
        exit_car_from_database(car, conn, cursor)

        # remove the car from the list
        cars.remove(car)

        # sleep for a random amount of time about 15 seconds give or take 5
        time.sleep(random.randint(5, 10))

        if len(cars) == 0:
            cursor.execute("SELECT * FROM dbo.ParkingGarage WHERE exit_time IS NULL")
            rows = cursor.fetchall()
            for row in rows:
                car = Car(plate=row.plate, entry_time=row.entry_time.strftime('%Y-%m-%d %H:%M:%S'), exit_time=row.exit_time)
                cars.append(car)

    # close the connection
    conn.close()

if __name__ == '__main__':
    print('The garage has opened.')
    # Create a multiprocessing queue
    q = multiprocessing.Queue()

    # Create a process to add cars to the database
    add_cars_process = multiprocessing.Process(target=create_cars, args=(q,))
    add_cars_process.start()

    # Create a process to remove cars from the database
    exit_cars_process = multiprocessing.Process(target=exit_cars)
    exit_cars_process.start()

    # Wait for the add cars process to finish
    add_cars_process.join()

    # Wait for the exit cars process to finish
    exit_cars_process.join()

    print('The garage has closed.')




