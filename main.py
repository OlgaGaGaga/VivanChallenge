import mysql.connector
import pandas as pd

def connect_db(in_host, in_user, in_passwd, in_db):
    mydb = mysql.connector.connect(
        host=in_host,
        user=in_user,
        passwd=in_passwd,
        database=in_db
    )

    return mydb


def read_json(file):
    the_file = pd.read_json(file)

    return the_file


if __name__ == '__main__':

    mydb = connect_db('localhost', 'root', 'kuznechik', 'testdb')
    mycursor = mydb.cursor()

    # create_db(mycursor, 'testdb')

    # mycursor.execute("CREATE DATABASE testdb")


    # mycursor.execute("SHOW DATABASES")
    #
    # for db in mycursor:
    #     print(db)


    # mycursor.execute("CREATE TABLE students (name VARCHAR(255), age INTEGER(10))")


    # mycursor.execute("SHOW TABLES")
    # for tb in mycursor:
    #     print(tb)


    # sqlFormula = 'INSERT INTO students (name, age) VALUES (%s, %s)'
    #
    # student1 = ("Rachel", 8)
    # mycursor.execute(sqlFormula, student1)
    # mydb.commit()


    json_file = read_json('../Downloads/Vivan/input_files/benchling_entries.json')

    print(json_file.head())

