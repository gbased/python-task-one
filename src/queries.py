from src.database import get_connection
import logging

class DatabaseQueries:
    def __init__(self):
        self.conn = get_connection
        self.cursor = self.conn.cursor()

    def get_room_student_counts(self):
        query = """
                SELECT
                    r.rooms  
                """