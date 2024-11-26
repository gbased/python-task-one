# src/queries.py

import psycopg
from database import get_connection
import logging

class DatabaseQueries:
    def __init__(self):
        self.conn = get_connection()

    def get_rooms_student_counts(self):
        logging.info('Executing the query: List of rooms and the number of students in each of them')
        query = """
            SELECT
                r.name AS room_name,
                COUNT(s.id) AS student_count
            FROM students AS s
            JOIN rooms AS r ON r.id = s.room
            GROUP BY r.id
            ORDER BY r.name
        """
        try:
            with self.conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                logging.info('Request completed successfully')
                return results
        except Exception as e:
            logging.error(f'Error when executing get_rooms_student_counts request: {e}')
            return []

    def get_rooms_with_smallest_avg_age(self):
        logging.info('Fulfilling the request: 5 rooms with the youngest student age.')
        query = """
            SELECT
	            r.name AS room_name,
	            AVG(EXTRACT(YEAR FROM AGE(current_date, birthday))) AS average_age
            FROM
	            students AS s
	            JOIN rooms AS r ON s.room = r.id
            GROUP BY r.name
            ORDER BY average_age LIMIT 5;
            """
        try:
            with self.conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                logging.info('Request completed successfully')
                return results
        except Exception as e:
            logging.error(f'Error when executing get_rooms_with_smallest_avg_age request: {e}')
            return []

    def get_rooms_with_largest_age_difference(self):
        logging.info('Fulfilling the query: 5 rooms with the largest difference in student age')
        query = """
                SELECT
	                r.name AS room_name,
	                MAX(EXTRACT(YEAR FROM AGE(current_date, s.birthday))) - MIN(EXTRACT(YEAR FROM AGE(current_date, s.birthday))) AS age_difference
                FROM
	                students AS s
	                JOIN rooms AS r ON r.id = s.room
                GROUP BY r.name
                ORDER BY age_difference DESC
                LIMIT 5
                """
        try:
            with self.conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                logging.info('Request completed successfully')
                return results
        except Exception as e:
            logging.error(f'Error when executing get_rooms_with_largest_age_difference: {e}')
            return []

    def get_rooms_with_both_sex_students(self):
        logging.info('Fulfilling the query: rooms with at least two students of each sex')
        query = """
            SELECT
                r.id,
                r.name,
                SUM(CASE WHEN s.sex = 'F' THEN 1 ELSE 0 END) AS female_count,
                SUM(CASE WHEN s.sex = 'M' THEN 1 ELSE 0 END) AS male_count
            FROM
                rooms r
                JOIN students s ON r.id = s.room
            GROUP BY
                r.id,
                r.name
            HAVING
                COUNT(DISTINCT s.sex) >= 2;
                """
        try:
            with self.conn.cursor(row_factory=psycopg.rows.dict_row) as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
                logging.info('Request completed successfully')
                return results
        except Exception as e:
            logging.error(f'Error when executing get_rooms_with_both_sex_students: {e}')
            return []

    def execute_all_queries(self):
        logging.info('Execution of all requests')
        return {
            'room_student_counts': self.get_rooms_student_counts(),
            'smallest_avg_age_rooms': self.get_rooms_with_smallest_avg_age(),
            'largest_age_diff_rooms': self.get_rooms_with_largest_age_difference(),
            'mixed_gender_rooms': self.get_rooms_with_both_sex_students()
        }

    def close_connection(self): 
        if self.conn:
            self.conn.close()
            logging.info('Connection to the database closed')
