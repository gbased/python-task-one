from database import get_connection
import json
import logging


class DataLoader:
    def __init__(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()

    def load_rooms(self, rooms_file):
        logging.info(f'Loading rooms from {rooms_file}')

        with open(rooms_file, 'r') as file:
            rooms = json.load(file)
            for room in rooms:
                try:
                    self.cursor.execute(
                    f"INSERT INTO rooms (id, name) VALUES ({room['id']}, {room['name']});"
                )
                except Exception as e:
                    logging.error(f'Error when inserting room: {e}')
            self.conn.commit()

            logging.info('Loading of rooms completed')

    def load_students(self, students_file):
        logging.info(f'Loading students from {rooms_file}')

        with open(students_file, 'r') as file:
            students = json.load(file)

            for student in students: 
                try:
                    self.cursor.execute(
                    f"""INSERT INTO rooms (id, name) VALUES
                    (
                        {student["id"]},
                        {student["birthday"]}, 
                        {student["room"]}, 
                        {student["name"]}, 
                        {student["sex"]});"
                )
                except Exception as e:
                    logging.error(f'Error when inserting student: {e}')
            self.conn.commit()

            logging.info('Loading of students completed')

