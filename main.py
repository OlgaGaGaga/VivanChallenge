import mysql.connector

def connect_db(in_host, in_user, in_passwd, in_db):
    mydb = mysql.connector.connect(
        host=in_host,
        user=in_user,
        passwd=in_passwd,
        database=in_db
    )

    return mydb


def init_cursor(mydb):
    mycursor = mydb.cursor()

    return mycursor

def cursor_execute(cursor, name):

    cursor.execute("CREATE DATABASE " + name)




if __name__ == '__main__':

    mydb = connect_db('localhost', 'root', 'kuznechik', 'testdb')
    mycursor = init_cursor(mydb=mydb)

    # create_db(mycursor, 'testdb')

    # mycursor.execute("CREATE DATABASE testdb")


    # mycursor.execute("SHOW DATABASES")
    #
    # for db in mycursor:
    #     print(db)


    # mycursor.execute("CREATE TABLE students (name VARCHAR(255), age INTEGER(10))")


    mycursor.execute("SHOW TABLES")
    for tb in mycursor:
        print(tb)

    # print(mydb)