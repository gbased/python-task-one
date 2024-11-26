# src/loader.py

import json
from database import get_connection
import logging
from datetime import datetime

class DataLoader:
    def __init__(self):
        self.conn = get_connection()
        self.conn.autocommit = True 

    def load_rooms(self, rooms_file):
        logging.info(f'Loading rooms from {rooms_file}')

        try:
            with open(rooms_file, 'r', encoding='utf-8') as file:
                rooms = json.load(file)
        except Exception as e:
            logging.error(f'Failed to read rooms file: {e}')
            return

        for room in rooms:
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO rooms (id, name)
                        VALUES (%s, %s)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        (room['id'], room['name'])
                    )
                logging.info(f"Inserted room: {room['name']}")
            except Exception as e:
                logging.error(f'Error when inserting room {room}: {e}')

        logging.info('Loading of rooms completed')

    def load_students(self, students_file):
        logging.info(f'Loading students from {students_file}')

        try:
            with open(students_file, 'r', encoding='utf-8') as file:
                students = json.load(file)
        except Exception as e:
            logging.error(f'Failed to read students file: {e}')
            return

        for student in students:
            birthdate_str = student.get("birthday", "")
            try:
                birthdate = datetime.strptime(birthdate_str.split("T")[0], '%Y-%m-%d').date()
            except ValueError as ve:
                logging.error(f"Invalid birthdate format for student {student['name']}: {birthdate_str}")
                continue  

            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        f"""
                        INSERT INTO students (id, birthday, name, room, sex)
                        VALUES ({student['id']}, '{birthdate}', '{student['name']}', {student['room']}, '{student['sex']}')
                        ON CONFLICT (id) DO NOTHING
                        """
                    )
                logging.info(f"Inserted student: {student['name']}")
            except Exception as e:
                logging.error(f'Error when inserting student {student}: {e}')

        logging.info('Loading of students completed')

