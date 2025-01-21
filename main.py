import psycopg2
from psycopg2.sql import SQL, Identifier
import configparser


config = configparser.ConfigParser()
config.read('settings.ini')
password_postgres = config['Password']['postgres']


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE client_Phone;
            DROP TABLE client_info
                """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS client_info(
            client_id SERIAL PRIMARY KEY NOT NULL UNIQUE,
            client_name VARCHAR(60) NOT NULL,
            client_surname VARCHAR(60) NOT NULL,
            client_email VARCHAR(100) UNIQUE
        );     
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS client_Phone(
            id SERIAL PRIMARY KEY,
            phone_number VARCHAR(12),
            client_id INTEGER REFERENCES client_info(client_id)
        );     
        """)
        conn.commit()

def add_new_client(conn, client_name, client_surname, client_email, phone_number = None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client_info(client_name, client_surname, client_email)
            VALUES (%s, %s, %s) RETURNING client_id, client_name, client_surname, client_email;
            """, (client_name, client_surname, client_email, ))
        return cur.fetchone()
        conn.commit()

def add_phone_number(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client_Phone(client_id, phone_number)
            VALUES (%s, %s) RETURNING client_id, phone_number;
        """, (client_id, phone_number, ))
        return cur.fetchone()
        conn.commit()

def update_client_info(conn, client_id, client_name = None, client_surname = None, client_email = None, phone_number = None):
    with conn.cursor() as cur:
        arg_list = {'client_name': client_name, 'client_surname': client_surname, 'client_email': client_email}
        for key, arg in arg_list.items():
            if arg:
                cur.execute(SQL('UPDATE client_info SET {}=%s WHERE client_id = %s').format(Identifier(key)),
                    (arg, client_id))
        cur.execute("""
            SELECT * FROM client_info
            WHERE client_id = %s;
            """, (client_id,))
        return cur.fetchall()
        conn.commit()

def delete_phone(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM client_phone
            WHERE client_id=%s;
            """, (client_id,))
        cur.execute("""
            SELECT * FROM client_phone;
        """)
        return cur.fetchone()
        conn.commit()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM client_info
            WHERE client_id=%s;
            """, (client_id,))
        cur.execute("""
            SELECT * FROM client_info;
        """)
        return cur.fetchone()
        conn.commit()


def find_client(conn, client_name = None, client_surname = None, client_email = None, phone_number = None):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT client_name, client_surname, client_email, phone_number From client_info c
			LEFT JOIN client_phone c2 ON c2.client_id = c.client_id
			WHERE client_name=%s OR client_surname=%s OR client_email=%s OR phone_number=%s;
			""", (client_name, client_surname, client_email, phone_number,))
        return cur.fetchone()




if __name__ == '__main__':
    with psycopg2.connect(database="clients", user="postgres", password=password_postgres) as conn:
        # print (add_new_client(conn,'Tom', 'Adoms', 'adom@mail.ru'))
        # print (add_new_client(conn,'Иван', 'Иванов', 'III@mail.ru'))
        # print(add_new_client(conn, "Петр", "Петров", "222@mail.ru"))
        # print (add_phone_number(conn, '3', '89109467816'))
        # print (add_phone_number(conn, '3', '89123456789'))
        # print (add_phone_number(conn, '32', '89111111111'))
        # print (update_client_info (conn, '28', 'Евгений', 'Иванов', '222@mail.com'))
        # print (update_client_info (conn, '28', 'Иван'))
        # print (delete_phone (conn, '3', '89123456789'))
        # print (delete_client(conn, '3'))
        print (find_client(conn, client_name="Иван"))



conn.close()