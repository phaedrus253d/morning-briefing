# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import os

def load_data():
    if not os.path.exists('./data/tables.txt'):
        if os.path.exists('./.database.db3'):
            convert_sql_to_pandas()
        else:
            print("No sqlite3 or pandas data found, exiting")
            exit()

def convert_sql_to_pandas():
    con = sqlite3.connect(".database.db3")
    sql_query = """SELECT name FROM sqlite_master  
      WHERE type='table';"""
    cur = con.cursor()
    cur.execute(sql_query)
    tables = cur.fetchall()
    tableList = []
    for table in tables:
        print("converting", table[0])
        pd_table = pd.read_sql_query("SELECT * from "+table[0], con)
        pd_table.to_pickle('./data/'+table[0]+'.pkl')
        tableList.append(table[0])
        
    with open('./data/tables.txt', 'w') as f:
        f.writelines(tableList)
    
