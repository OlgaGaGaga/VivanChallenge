import mysql.connector

def connect_db(in_host, in_user, in_passwd):
    mydb = mysql.connector.connect(
        host=in_host,
        user=in_user,
        passwd=in_passwd
    )

    return mydb





if __name__ == '__main__':

    mydb = connect_db('localhost', 'root', 'kuznechik')

    print(mydb)