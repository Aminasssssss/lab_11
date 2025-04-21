import psycopg2
import csv
import pandas as pd
from tabulate import tabulate 

conn = psycopg2.connect(host="localhost", dbname = "lab10", user = "aminazhumatayeva", password = "", port = 5432)
cur = conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS phonebook (
      user_id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      surname VARCHAR(255) NOT NULL, 
      phone VARCHAR(255) NOT NULL
)
""")



cur.execute("""
CREATE OR REPLACE FUNCTION search_pattern(pat TEXT)
RETURNS TABLE(user_id INT, name TEXT, surname TEXT, phone TEXT) AS $$
BEGIN
    RETURN QUERY SELECT * FROM phonebook 
    WHERE name ILIKE '%' || pat || '%' 
       OR surname ILIKE '%' || pat || '%'
       OR phone ILIKE '%' || pat || '%';
END;
$$ LANGUAGE plpgsql;
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE insert_or_update_user(new_name TEXT, new_surname TEXT, new_phone TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM phonebook WHERE name = new_name AND surname = new_surname) THEN
        UPDATE phonebook SET phone = new_phone WHERE name = new_name AND surname = new_surname;
    ELSE
        INSERT INTO phonebook(name, surname, phone) VALUES (new_name, new_surname, new_phone);
    END IF;
END;
$$;
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE bulk_insert_users(names TEXT[], surnames TEXT[], phones TEXT[], OUT invalid_data TEXT[])
LANGUAGE plpgsql
AS $$
DECLARE
    i INT;
    phone_pattern TEXT := '^[0-9]+$';
BEGIN
    invalid_data := ARRAY[]::TEXT[];
    FOR i IN 1..array_length(names, 1) LOOP
        IF phones[i] ~ phone_pattern THEN
            CALL insert_or_update_user(names[i], surnames[i], phones[i]);
        ELSE
            invalid_data := array_append(invalid_data, names[i] || ' ' || surnames[i] || ' → ' || phones[i]);
        END IF;
    END LOOP;
END;
$$;
""")

cur.execute("""
CREATE OR REPLACE FUNCTION paginated_query(limit_count INT, offset_count INT)
RETURNS TABLE(user_id INT, name TEXT, surname TEXT, phone TEXT) AS $$
BEGIN
    RETURN QUERY SELECT * FROM phonebook ORDER BY user_id LIMIT limit_count OFFSET offset_count;
END;
$$ LANGUAGE plpgsql;
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE delete_by_name_or_phone(target TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM phonebook WHERE name = target OR surname = target OR phone = target;
END;
$$;
""")

conn.commit()



filepath = "/Users/aminazhumatayeva/Documents/lab10/student.csv"  
with open(filepath, 'r') as f:
    next(f)
    reader = csv.reader(f)
    for row in reader:
        cur.execute("INSERT INTO phonebook (name, surname, phone) VALUES (%s, %s, %s)", (row[0], row[1], row[2]))

conn.commit()

check = True
command = ''
temp = ''

name_var = ''
surname_var = ''
phone_var = ''
id_var = ''

start = True
back = False

back_com = ''
name_upd = ''
surname_upd = ''
phone_upd = ''

filepath = ''

while check:
    if start == True or back == True:
        start = False
        print("""
        List of the commands:
        1. Type "i" or "I" in order to INSERT data to the table.
        2. Type "u" or "U" in order to UPDATE data in the table.
        3. Type "q" or "Q" in order to make specidific QUERY in the table.
        4. Type "d" or "D" in order to DELETE data from the table.
        5. Type "f" or "F" in order to close the program.
        6. Type "s" or "S" in order to see the values in the table.
        7. Type "pattern" to search by pattern.
        8. Type "one" to insert or update one user.
        9. Type "bulk" to bulk insert with validation.
       10. Type "page" to view paginated data.
        """)

        command = str(input())
        
        
        if command == "i" or command == "I":
            print('Type "csv" or "con" to choose option between uploading csv file or typing from console: ')
            command = ''
            temp = str(input())
            if temp == "con":
                name_var = str(input("Name: "))
                surname_var = str(input("Surname: "))
                phone_var = str(input("Phone: "))
                cur.execute("INSERT INTO phonebook (name, surname, phone) VALUES (%s, %s, %s)", (name_var, surname_var, phone_var))
                conn.commit()
                back_com = str(input('Type "back" in order to return to the list of the commands: '))
                if back_com == "back":
                    back = True
            if temp == "csv":
                filepath = input("Enter a file path with proper extension: ")
                with open(str(filepath), 'r') as f:
                
                    reader = csv.reader(f)
                    next(reader)
                    for row in reader:
                        cur.execute("INSERT INTO phonebook (name, surname, phone) VALUES (%s, %s, %s)", (row[0], row[1], row[2]))
                conn.commit()

                    
                back_com = str(input('Type "back" in order to return to the list of the commands: '))
                if back_com == "back":
                    back = True

        
        if command == "d" or command == "D":
            back = False
            command = ''
            phone_var = str(input('Type phone number which you want to delete: '))
            cur.execute("DELETE FROM phonebook WHERE phone = %s", (phone_var,))
            conn.commit()
            back_com = str(input('Type "back" in order to return to the list of the commands: '))
            if back_com == "back":
                back = True
        
        
        if command == 'u' or command == 'U':
            back = False
            command = ''
            temp = str(input('Type the name of the column that you want to change: '))
            if temp == "name":
                name_var = str(input("Enter name that you want to change: "))
                name_upd = str(input("Enter the new name: "))
                cur.execute("UPDATE phonebook SET name = %s WHERE name = %s", (name_upd, name_var))
                conn.commit()
                back_com = str(input('Type "back" in order to return to the list of the commands: '))
                if back_com == "back":
                    back = True

            if temp == "surname":
                surname_var = str(input("Enter surname that you want to change: "))
                surname_upd = str(input("Enter the new surname: "))
                cur.execute("UPDATE phonebook SET surname = %s WHERE surname = %s", (surname_upd, surname_var))
                back_com = str(input('Type "back" in order to return to the list of the commands: '))
                if back_com == "back":
                    back = True

            if temp == "phone":
                name_var = str(input("Enter phone number that you want to change: "))
                name_upd = str(input("Enter the new phone number: "))
                cur.execute("UPDATE phonebook SET phone = %s WHERE phone = %s", (phone_upd, phone_var))
                back_com = str(input('Type "back" in order to return to the list of the commands: '))
                if back_com == "back":
                    back = True
        
        
        if command == "q" or command == "Q":
            back = False
            command = ''
            temp = str(input("Type the name of the column which will be used for searching data: "))
            if temp == "id":
                id_var = str(input("Type id of the user: "))
                cur.execute("SELECT * FROM phonebook WHERE user_id = %s", (id_var, ))
                rows = cur.fetchall()
                print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"]))
                back_com = str(input('Type "back" in order to return to the list of the commands: '))
                if back_com == "back":
                    back = True
            
            if temp == "name":
                name_var = str(input("Type name of the user: "))
                cur.execute("SELECT * FROM phonebook WHERE name = %s", (name_var, ))
                rows = cur.fetchall()
                print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"]))
                back_com = str(input('Type "back" in order to return to the list of the commands: '))
                if back_com == "back":
                    back = True
            
            if temp == "surname":
                surname_var = str(input("Type surname of the user: "))
                cur.execute("SELECT * FROM phonebook WHERE surname = %s", (surname_var, ))
                rows = cur.fetchall()
                print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"]))
                back_com = str(input('Type "back" in order to return to the list of the commands: '))
                if back_com == "back":
                    back = True
                
            if temp == "phone":
                phone_var = str(input("Type phone number of the user: "))
                cur.execute("SELECT * FROM phonebook WHERE phone = %s", (phone_var, ))
                rows = cur.fetchall()
                print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"]))
                back_com = str(input('Type "back" in order to return to the list of the commands: '))
                if back_com == "back":
                    back = True

        
        
        if command == "s" or command == "S":
            back = False
            command = ''
            cur.execute("SELECT * from phonebook;")
            rows = cur.fetchall()
            print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"], tablefmt='fancy_grid'))
            back_com = str(input('Type "back" in order to return to the list of the commands: '))
            if back_com == "back":
                back = True
        

        if command == "pattern":
            pattern = input("Enter pattern to search: ")
            cur.execute("SELECT * FROM search_pattern(%s)", (pattern,))
            print(tabulate(cur.fetchall(), headers=["ID", "Name", "Surname", "Phone"], tablefmt='grid'))

        if command == "one":
            name = input("Enter name: ")
            surname = input("Enter surname: ")
            phone = input("Enter phone: ")
            cur.execute("CALL insert_or_update_user(%s, %s, %s)", (name, surname, phone))
            conn.commit()

        if command == "bulk":
            count = int(input("Enter how many users you want to add: "))
            names, surnames, phones = [], [], []
            for _ in range(count):
                names.append(input("Name: "))
                surnames.append(input("Surname: "))
                phones.append(input("Phone: "))
            cur.execute("CALL bulk_insert_users(%s, %s, %s, %s)", (names, surnames, phones, []))
            conn.commit()

        if command == "page":
            limit = int(input("Enter limit: "))
            offset = int(input("Enter offset: "))
            cur.execute("SELECT * FROM paginated_query(%s, %s)", (limit, offset))
            print(tabulate(cur.fetchall(), headers=["ID", "Name", "Surname", "Phone"], tablefmt='grid'))

        if command == "delete":
            val = input("Enter name, surname, or phone to delete: ")
            cur.execute("CALL delete_by_name_or_phone(%s)", (val,))
            conn.commit()
            
        if command == "f" or command == "F":
            command = ''
            check = False

conn.commit()
cur.close()
conn.close()
