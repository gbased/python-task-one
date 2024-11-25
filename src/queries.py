from src.database import get_connection
import logging

class DatabaseQueries:
    def __init__(self):
        self.conn = get_connection
        self.cursor = self.conn.cursor(dictionary=True)

    def get_room_student_counts(self):
        query = """
                SELECT
                    r.id,
                    COUNT(s.id) AS student_count
                FROM students AS s
                JOIN rooms AS r ON r.id = s.room
                GROUP BY r.id
                """
        self.cursor.execute(query)
        return self.cursor.fetchall()

oq = DatabaseQueries()
print(oq.get_room_student_counts())