import pyodbc
import pandas as pd
import pydantic
import random
import datetime
import time
import multiprocessing
import requests
import math
from config import server, database, username, password, pbiurl

# columns in the parking-garage table
# columns = ['plate', 'entry_time', 'exit_time']

# global variable to keep track of the garage being open
garage_capacity = 145   # number of cars that can fit in the garage
gate_speed = 5          # average time it takes for a car to enter the garage
gate_speed_std_dev = 1  # standard deviation of the time it takes for a car to enter the garage
arrival_average = 15     # average time between cars arriving
arrival_std_dev = 4     # standard deviation of the time between cars arriving
exit_average = 14
exit_std_dev = 13
end_time = 10           # 24hr eg 9 = 9am, 17 = 5pm
pbi_push_interval = 2   # push to PowerBi every # seconds

# create car class
class Car(pydantic.BaseModel):
    plate: str
    entry_time: str | None
    exit_time: str | None

# create a connection to the database
def create_connection():
    conn = pyodbc.connect(f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;')
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

def create_cars(q, end, line_of_cars, run_time=600):
    print('Creating cars')

    time_remaining = True
    start_time = datetime.datetime.now()
    while time_remaining:
        # create a random car plate 6 characters long
        plate = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890', k=6))
        entry_time = None
        exit_time = None

        # create a car object, make the times strings
        car = Car(plate=plate, entry_time=entry_time, exit_time=exit_time)
        print(f'Car created: {car}')

        # add the car to the line of cars
        line_of_cars.append(car)

        # sleep for a normal distribution of time about 5 seconds give or take 2 seconds
        # it takes this long for new cars to show up
        time.sleep(math.fabs(random.normalvariate(arrival_average, arrival_std_dev)))

        # determine if the time has run out
        # if (datetime.datetime.now() - start_time).seconds > run_time:
        #    time_remaining = False

        # set time remaining to false if is 5pm or later
        # if datetime.datetime.now().hour >= end_time:
        #     time_remaining = False

    end.set()


def enter_cars(q, end, line_of_cars):
    print('Entering cars')

    # create a connection to the database
    conn = pyodbc.connect(f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;')
    cursor = conn.cursor()

    # get the number of cars in the garage
    cursor.execute("SELECT COUNT(*) FROM dbo.ParkingGarage WHERE exit_time IS NULL")
    count = cursor.fetchone()[0]
    number_of_open_spots = garage_capacity - count

    # let a car in every 5 seconds
    while not end.is_set():
        if len(line_of_cars) > 0:
            if number_of_open_spots > 0:
                # get the car from the line of cars
                car = line_of_cars.pop(0)
                # add the car to the database
                car.entry_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                add_car_to_database(car, conn, cursor)

                # add the event to the queue
                # with plate, type, and timestamp
                # type is either entry or exit
                q.put([car.plate, 'entry', car.entry_time])

                # decrement the number of open spots
                number_of_open_spots -= 1

                time.sleep(math.fabs(random.normalvariate(gate_speed, gate_speed_std_dev)))
            else:
                print('No open spots')

                # sleep for 1 second
                time.sleep(math.fabs(random.normalvariate(gate_speed, gate_speed_std_dev)))

                # get the number of cars in the garage
                cursor.execute("SELECT COUNT(*) FROM dbo.ParkingGarage WHERE exit_time IS NULL")
                count = cursor.fetchone()[0]
                number_of_open_spots = garage_capacity - count


    # close the connection
    conn.close()

def exit_cars(q, end):
    print('Exiting cars')

    # create a connection to the database
    conn = pyodbc.connect(f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;')
    cursor = conn.cursor()

    cars = []
    # get all cars from the database with no exit time
    cursor.execute("SELECT * FROM dbo.ParkingGarage WHERE exit_time IS NULL")
    rows = cursor.fetchall()
    for row in rows:
        car = Car(plate=row.plate, entry_time=row.entry_time.strftime('%Y-%m-%d %H:%M:%S'), exit_time=row.exit_time)
        cars.append(car)

    while not end.is_set():
        if len(cars) > 0:
            # pick a random car and have it exit
            car = random.choice(cars)
            car.exit_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # exit the car from the database
            exit_car_from_database(car, conn, cursor)

            # remove the car from the list
            cars.remove(car)

            # add the event to the queue
            # with plate, type, and timestamp
            # type is either entry or exit
            q.put([car.plate, 'exit', car.exit_time])

            time.sleep(math.fabs(random.normalvariate(exit_average, exit_std_dev)))
        else:
            cursor.execute("SELECT * FROM dbo.ParkingGarage WHERE exit_time IS NULL")
            rows = cursor.fetchall()
            for row in rows:
                car = Car(plate=row.plate, entry_time=row.entry_time.strftime('%Y-%m-%d %H:%M:%S'), exit_time=row.exit_time)
                cars.append(car)

            time.sleep(1)
    # close the connection
    conn.close()

def push_data(q, end, line_of_cars):
    print('Pushing data to PowerBI')
    REST_API_URL = pbiurl

    number_of_cars = 0

    # get the current cars in the garage
    # trust the cert
    conn = pyodbc.connect(f'DRIVER=ODBC Driver 18 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM dbo.ParkingGarage WHERE exit_time IS NULL")
    rows = cursor.fetchall()
    number_of_cars = len(rows)
    conn.close()

    while not end.is_set():
        # get all events from the queue
        events = []
        while not q.empty():
            events.append(q.get())

        # update the cars in the garage
        for event in events:
            if event[1] == 'entry':
                number_of_cars += 1
            elif event[1] == 'exit':
                number_of_cars -= 1

        # print the number of cars in the garage
        # print(f'Cars in garage: {len(cars_in_garage)}')

        # send number_of_cars to PowerBI, along with the current timestamp
        data = {'number_of_cars': number_of_cars,
                'time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'capacity-min': 0,
                'capacity-max': garage_capacity,
                'length_of_line': len(line_of_cars)
                }
        response = requests.post(REST_API_URL, json=data)
        # print(response.status_code)

        # print(response.status_code, data')

        time.sleep(pbi_push_interval)


if __name__ == '__main__':
    print('The garage has opened.')
    # Create a multiprocessing queue for events to push to PowerBI
    q = multiprocessing.Queue()
    end = multiprocessing.Event()
    line_of_cars = multiprocessing.Manager().list()

    # Create a process to create cars
    # also adds the event to the queue
    add_cars_process = multiprocessing.Process(target=create_cars, args=(q,end,line_of_cars,))
    add_cars_process.start()

    # Create a process to enter cars
    # also adds the event to the queue
    enter_cars_process = multiprocessing.Process(target=enter_cars, args=(q,end,line_of_cars,))
    enter_cars_process.start()


    # Create a process to remove cars from the database
    # also adds the event to the queue
    exit_cars_process = multiprocessing.Process(target=exit_cars, args=(q,end,))
    exit_cars_process.start()

    # Create a process to push data to PowerBI
    # pulls events from the queue
    push_data_process = multiprocessing.Process(target=push_data, args=(q,end,line_of_cars,))
    push_data_process.start()

    # Wait for the add cars process to finish
    add_cars_process.join()
    print('Cars have stopped entering the garage.')

    # Wait for the enter cars process to finish
    enter_cars_process.join()
    print('Cars have stopped entering the garage.')

    # Wait for the exit cars process to finish
    exit_cars_process.join()
    print('Cars have stopped exiting the garage.')

    push_data_process.join()
    print('PowerBI push has finished.')





