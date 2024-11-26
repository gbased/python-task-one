CREATE TABLE IF NOT EXISTS rooms (
    id INT PRIMARY KEY,
    name VARCHAR(100)
);

CREATE TYPE SEX_TYPE AS ENUM('F', 'M');

CREATE TABLE IF NOT EXISTS students (
    id INT PRIMARY KEY,
    birthday TIMESTAMP,
    name VARCHAR(100),
    room INT,
    sex SEX_TYPE,
    FOREIGN KEY (room) REFERENCES rooms(id)
);
