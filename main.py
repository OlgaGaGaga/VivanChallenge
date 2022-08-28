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

def create_db(name, cursor):
    cursor.execute("CREATE DATABASE "+name)

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
                                       'text'].replace('Hs-', '') + ';'
                        upDmgene = upDmgene + \
                                   json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][1][
                                       'text'].replace('Dm-', '') + ';'
                        upDELDUPL = upDELDUPL + \
                                    json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][2][
                                        'text'] + ';'
                        upConfidence = upConfidence + \
                                       json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][3][
                                           'text'] + ';'
                        upComments = upComments + \
                                     json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][4][
                                         'text'] + ';'

                if json_data['entries'][n]['days'][0]['notes'][i]['table']['name'] == 'Genes to down regulate':

                    for j in range(len(json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'])):
                        downHsgene = downHsgene + \
                                     json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][0][
                                         'text'].replace('Hs-', '') + ';'
                        downDmgene = downDmgene + \
                                     json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][1][
                                         'text'].replace('Dm-', '') + ';'
                        downDELDUPL = downDELDUPL + \
                                      json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][2][
                                          'text'] + ';'
                        downConfidence = downConfidence + \
                                         json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][3][
                                             'text'] + ';'
                        downComments = downComments + \
                                       json_data['entries'][n]['days'][0]['notes'][i]['table']['rows'][j]['cells'][4][
                                           'text'] + ';'


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

    table_up = table.iloc[:, :6]
    table_down = table.iloc[:,[0,6, 7, 8, 9, 10]]

    new_table_up = pd.DataFrame.from_dict(
        {'patID': [], 'upHsgene': [], 'upDmgene': [], 'upDELDUPL': [], 'upConfidence': [],
         'upComments': []})
    for i in range(table_up.shape[0]):
        if ';' not in table_up.iloc[i, 1]:
            new_table_up = pd.concat([new_table_up, pd.DataFrame(table_up.iloc[[i], :])], ignore_index=True)
        else:

            patID = table_up.iloc[i, 0]
            upHsgene = table_up.iloc[i, 1].split(';')
            upDmgene = table_up.iloc[i, 2].split(';')
            upDELDUPL = table_up.iloc[i, 3].split(';')
            upConfidence = table_up.iloc[i, 4].split(';')
            upComments = table_up.iloc[i, 5].split(';')

            sep_table = pd.DataFrame.from_dict({'patID': [patID] * len(upHsgene), 'upHsgene': upHsgene, \
                                                'upDmgene': upDmgene, 'upDELDUPL': upDELDUPL, \
                                                'upConfidence': upConfidence, 'upComments': upComments})

            new_table_up = pd.concat([new_table_up, sep_table], ignore_index=True)

    new_table_up = new_table_up.replace('', 'To be filled')
    new_table_up = new_table_up.fillna('To be filled')

    new_table_down = pd.DataFrame.from_dict(
        {'patID': [], 'downHsgene': [], 'downDmgene': [], 'downDELDUPL': [], 'downConfidence': [],
         'downComments': []})
    for i in range(table_down.shape[0]):
        if ';' not in table_down.iloc[i, 1]:
            new_table_down = pd.concat([new_table_down, pd.DataFrame(table_down.iloc[[i], :])], ignore_index=True)
        else:

            patID = table_down.iloc[i, 0]
            downHsgene = table_down.iloc[i, 1].split(';')
            downDmgene = table_down.iloc[i, 2].split(';')
            downDELDUPL = table_down.iloc[i, 3].split(';')
            downConfidence = table_down.iloc[i, 4].split(';')
            downComments = table_down.iloc[i, 5].split(';')



            sep_table = pd.DataFrame.from_dict({'patID': [patID] * len(downHsgene), 'downHsgene': downHsgene, \
                                                'downDmgene': downDmgene, 'downDELDUPL': downDELDUPL, \
                                                'downConfidence': downConfidence, 'downComments': downComments})

            new_table_down = pd.concat([new_table_down, sep_table], ignore_index=True)

    new_table_down = new_table_down.replace('', 'To be filled')
    new_table_down = new_table_down.fillna('To be filled')


    return (new_table_up, new_table_down)

def read_cnv(file):
    table = pd.read_csv(file, sep='\t')
    return table

def fast_insert_data_in_db(connection, cursor, db_name, table_name, dict_col_type, pd_df):
    #create table
    string_col_type = table_name + '('
    for key in dict_col_type.keys():
        string_col_type = string_col_type + key + ' ' + dict_col_type[key] +', '

    string_col_type=string_col_type[:-2]
    string_col_type += ')'

    cursor.execute("CREATE TABLE " + string_col_type)

    formula = '(' + ('%s,' * pd_df.shape[1])
    formula = formula[:-1] + ')'

    list_rows = []
    for i, row in pd_df.iterrows():
        list_rows.append(tuple(row))
    print(len(list_rows))
    sql = "INSERT INTO " + str(db_name) + '.' + str(table_name) + " VALUES " + formula

    cursor.executemany(sql, list_rows)
    connection.commit()


if __name__ == '__main__':

    mydb = connect_db('localhost', 'root', 'password', 'Vivan')
    mycursor = mydb.cursor()


    # create_db('Vivan', mycursor)

    # mycursor.execute("SHOW DATABASES")
    #
    # for db in mycursor:
    #     print(db)



    # dict_col_type_json = {'patID': 'VARCHAR(255)', 'upHsgene': 'VARCHAR(255)',\
    # 'upDmgene': 'VARCHAR(255)', 'upDELDUPL': 'VARCHAR(255)', 'upConfidence': 'VARCHAR(255)', \
    # 'upComments': 'VARCHAR(255)', 'downHsgene': 'VARCHAR(255)', 'downDmgene': 'VARCHAR(255)', \
    # 'downDELDUPL': 'VARCHAR(255)', 'downConfidence': 'VARCHAR(255)', 'downComments': 'VARCHAR(255)'}
    #
    # insert_data_in_db(mydb, mycursor, 'Vivan', 'benchling', dict_col_type_json , read_process_json('../Downloads/Vivan/input_files/benchling_entries.json'))


    # dict_col_type_cnv = {'seqnames': 'VARCHAR(255)' , 'start': 'INTEGER(10)',\
    #    'end': 'INTEGER(10)', 'width': 'INTEGER(10)', 'type_alternation': 'VARCHAR(255)', \
    #    'copy_ratio': 'NUMERIC(10,6)', 'log_copy_ratio': 'NUMERIC(10,6)', \
    #    'copy_number': 'INTEGER(10)', 'length': 'INTEGER(10)', 'file_name': 'VARCHAR(255)', \
    #    'pipeline_name': 'VARCHAR(255)', 'flank_geneIds': 'INTEGER(10)', \
    #    'symbol': 'VARCHAR(255)', 'list_predicting_tools': 'TEXT',\
    #    'number_predicting_tools': 'INTEGER(10)', 'oncoKB_classification': 'VARCHAR(255)',\
    #    'oncoKB_classification_binary': 'INTEGER(10)', 'Patient_ID': 'VARCHAR(255)'}
    #
    # fast_insert_data_in_db(mydb, mycursor, 'Vivan', 'copies', dict_col_type_cnv,\
    # read_cnv('../Downloads/Vivan/input_files/cnv_processed_nan_processed.txt'))



    # table_up, table_down = read_process_json('../Downloads/Vivan/input_files/benchling_entries.json')
    #
    # dict_col_type_up = {'patID': 'VARCHAR(255)', 'upHsgene': 'VARCHAR(255)',\
    # 'upDmgene': 'VARCHAR(255)', 'upDELDUPL': 'VARCHAR(255)', 'upConfidence': 'VARCHAR(255)', \
    # 'upComments': 'VARCHAR(255)'}
    #
    # dict_col_type_down  = {'patID': 'VARCHAR(255)',  'downHsgene': 'VARCHAR(255)', 'downDmgene': 'VARCHAR(255)', \
    # 'downDELDUPL': 'VARCHAR(255)', 'downConfidence': 'VARCHAR(255)', 'downComments': 'VARCHAR(255)'}
    #
    # fast_insert_data_in_db(mydb, mycursor, 'Vivan', 'upGenes', dict_col_type_up,\
    # table_up)
    #
    # fast_insert_data_in_db(mydb, mycursor, 'Vivan', 'downGenes', dict_col_type_down, \
    #                        table_down)

    #









