#!/usr/bin/env python
#-*- encoding: utf-8 -*-
# Exporta Dados de Arquivos CSV Para o Oracle
# * Par 

import sys, csv
from decimal import Decimal
from datetime import datetime

def read_columns(cursor, tablename):
    vSQL = '''SELECT cols.column_name
                 FROM all_constraints cons, all_cons_columns cols
                 WHERE cols.table_name = '{0}'
                 AND cons.constraint_type = 'P'
                 AND cons.constraint_name = cols.constraint_name
                 AND cols.owner='DBATEZ'
                 AND cons.owner = cols.owner
            '''.format(tablename)
    cursor.execute(vSQL)
    pKey = cursor.fetchone()[0]

    vSQL = '''
       SELECT COLUMN_NAME, DATA_PRECISION, DATA_SCALE, DATA_TYPE, NULLABLE
              FROM USER_TAB_COLUMNS
              WHERE TABLE_NAME='{0}' ORDER BY COLUMN_ID 
           '''.format(tablename)
    cursor.execute(vSQL)     # Retrieve columns
    columns = cursor.fetchall()
    return (pKey, columns) 

def create_tabletemp(cursor, tablename):
                          # Check Table if exists  
    vSQL='''SELECT  TABLE_NAME FROM USER_TABLES
                           WHERE TABLE_NAME = '{0}_TEMP'
         '''.format(tablename)
    cursor.execute(vSQL)
    if not cursor.fetchone():
        vSQL='''CREATE GLOBAL TEMPORARY TABLE {0}_TEMP AS
               (SELECT * FROM {1} WHERE 1=2)
             '''.format(tablename, tablename)
        cursor.execute(vSQL)
        print '%s_TEMP TEMPORARY TABLE HAS BEEN CREATED.' % tablename

def validFields(inFile):
    """
    table_columns -> 
    [('NR_CRM', None, None, 'VARCHAR2','Y'),('ID',None, None,'VARCHAR2','Y')]
    """
    L = []
    with open(inFile, 'rb') as f:
       reader = csv.reader(f, delimiter=';', quotechar='"')
       for row in reader:
           for value in enumerate(list(row)):
               pos = value[0]
               print value[1]
               if isinstance(eval(str(value[1])), int):
                   print 'Eh inteiro....'
               print row[pos],"@",type(row[pos])

def mergeTable(tableName, pKey, table_columns):
    vSQL = '''MERGE INTO {0} TAB_A
                    USING {1}_TEMP TAB_B ON
                    (TAB_A.{2} = TAB_B.{3})
              WHEN MATCHED THEN
                   UPDATE SET\n'''.format(tableName, tableName, pKey, pKey)

    totalFields = len(table_columns)
    conta = 0
    for column in table_columns:     # UPDATE
        conta += 1
	if column[0] == vKey: continue

	if conta <> totalFields:
            vSQL += '     TAB_A.%s = TAB_B.%s, \n ' % (column[0], column[0])
        else:
            vSQL += '     TAB_A.%s = TAB_B.%s '     % (column[0], column[0])

    vSQL += '''\nWHEN NOT MATCHED THEN
                    INSERT ( \n'''

    conta = 0
    for column in table_columns:     # INSERT - COLUMNS
        conta += 1
        if conta <> totalFields:
            vSQL += '     %s ,\n ' % (column[0])
        else:
            vSQL += '     %s  ) '  % (column[0])

    conta = 0
    vSQL += ''' VALUES ( '''
    for column in table_columns:     # INSERT - VALUES
        conta += 1
        if conta <> totalFields:
            vSQL += '     TAB_B.%s,\n ' % (column[0])
        else:
            vSQL += '     TAB_B.%s ) '  % (column[0])

    return vSQL

if __name__ == '__main__':
    infile   = sys.argv[1]  # Input file CSV
    validFields(infile)
