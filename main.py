import mysql.connector
import pandas as pd
import json
def connect_db(in_host, in_user, in_passwd, in_db):
    mydb = mysql.connector.connect(
        host=in_host,
        user=in_user,
        passwd=in_passwd,
        database=in_db
    )

    return mydb


def read_process_json(file):
    with open(file, 'r') as f:
        json_data = json.load(f)

    table_dict = {'patID': [], 'upHsgene': [], 'upDmgene': [], 'upDELDUPL': [], 'upConfidence': [], 'upComments': [],
                  'downHsgene': [], 'downDmgene': [], 'downDELDUPL': [], 'downConfidence': [], 'downComments': []}

    for n in range(len(json_data['entries'])):

        patID = json_data['entries'][n]['name'][:6]

        upHsgene = ''
        upDmgene = ''
        upDELDUPL = ''
        upConfidence = ''
        upComments = ''
        downHsgene = ''
        downDmgene = ''
        downDELDUPL = ''
        downConfidence = ''
        downComments = ''

        for i in range(len(json_data['entries'][n]['days'][0]['notes'])):
            if 'table' in json_data['entries'][n]['days'][0]['notes'][i].keys():

                if json_data['entries'][n]['days'][0]['notes'][i]['table']['name'] == 'Genes to up regulate':

                    for j in range(len(json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'])):
                        upHsgene = upHsgene + \
                                   json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][0][
                                       'text'].replace('Hs-', '') + ','
                        upDmgene = upDmgene + \
                                   json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][1][
                                       'text'].replace('Dm-', '') + ','
                        upDELDUPL = upDELDUPL + \
                                    json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][2][
                                        'text'] + ','
                        upConfidence = upConfidence + \
                                       json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][3][
                                           'text'] + ','
                        upComments = upComments + \
                                     json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][4][
                                         'text'] + ','

                if json_data['entries'][n]['days'][0]['notes'][i]['table']['name'] == 'Genes to down regulate':

                    for j in range(len(json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'])):
                        downHsgene = downHsgene + \
                                     json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][0][
                                         'text'].replace('Hs-', '') + ','
                        downDmgene = downDmgene + \
                                     json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][1][
                                         'text'].replace('Dm-', '') + ','
                        downDELDUPL = downDELDUPL + \
                                      json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][2][
                                          'text'] + ','
                        downConfidence = downConfidence + \
                                         json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][3][
                                             'text'] + ','
                        downComments = downComments + \
                                       json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][4][
                                           'text'] + ','


        table_dict['patID'].append(patID)
        table_dict['upHsgene'].append(upHsgene[:-1])
        table_dict['upDmgene'].append(upDmgene[:-1])
        table_dict['upDELDUPL'].append(upDELDUPL[:-1])
        table_dict['upConfidence'].append(upConfidence[:-1])
        table_dict['upComments'].append(upComments[:-1])
        table_dict['downHsgene'].append(downHsgene[:-1])
        table_dict['downDmgene'].append(downDmgene[:-1])
        table_dict['downDELDUPL'].append(downDELDUPL[:-1])
        table_dict['downConfidence'].append(downConfidence[:-1])
        table_dict['downComments'].append(downComments[:-1])

    table = pd.DataFrame.from_dict(table_dict)
    return table

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


    table = read_process_json('../Downloads/Vivan/input_files/benchling_entries.json')

    print(table)

