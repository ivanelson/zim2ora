#!/usr/bin/env python
#-*- encoding: utf-8 -*-
# Exporta Dados de Arquivos CSV Para o Oracle
# * Campos de Data Devem ser validados antes de ser importados.

import cx_Oracle as dbatez
import sys, csv
from decimal import Decimal
from datetime import datetime

def validFields(inFile, tableName, table_columns):
    """
    table_columns -> [('NR_CRM', None, None, 'VARCHAR2','Y'),('ID',None, None,'VARCHAR2','Y')]
    """

    L = []
    columns = [ col[0] for col in table_columns ]
    with open(inFile, 'rb') as f:
       reader = csv.reader(f, delimiter=';', quotechar='"')
       for row in reader:
           for value in enumerate(list(row)):
               pos = value[0]
               if table_columns[pos][4] == 'N' and \
                     value[1] in (None,'',' '):        # Not NUllable 
                         if table_columns[pos][3] == 'VARCHAR2':
                             row[pos] = ' '
                         elif table_columns[pos][3] == 'DATE':
                                     row[pos] = None
                         elif table_columns[pos][3] == 'NUMBER':
                                     row[pos] = Decimal(0)
               elif table_columns[pos][3] == 'NUMBER' and \
                       value[1] in (None, '',' '):
                           row[pos] = Decimal(0)
               elif table_columns[pos][3] == 'NUMBER' and \
                       table_columns[pos][2] > 0 and      \
                       value[1] not in ('',' ',None):
                           row[pos] = Decimal(row[pos])
               elif table_columns[pos][3] == 'DATE' and \
                       value[1] not in (None, 0, '0','',' '): # Invalid date
                   try:
                       datetime(value[1])
                   except TypeError:
                       row[pos] = None

               elif table_columns[pos][3] == 'DATE' and \
                     value[1] in (None,0,'0','',' '):
                         row[pos] = None
           L.append(tuple(row))

    sSQL = ('INSERT INTO {0}_TEMP (%s) VALUES (%s)'.format(tableName) %
           (','.join('%s' % name[0] for name in table_columns),
             ','.join(
                       "NVL(:{0},null)".format(i + 1) \
                       if str(value[3])=='DATE' else \
                          ':{0}'.format(i + 1)  \
                       for i, value in enumerate(list(table_columns)) )))
    return (sSQL, L)

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
    vTable   = sys.argv[1]  # Table Name in Oracle
    infile   = sys.argv[2]  # Input file CSV
    user     = sys.argv[3]
    password = sys.argv[4]
    host     = sys.argv[5]
    db = dbatez.connect("{}/{}@{}/orcl".format(user, password, host))
    cursor = db.cursor()
                          # Check Table if exists  
    vSQL='''SELECT  TABLE_NAME FROM USER_TABLES
                           WHERE TABLE_NAME = '{0}_TEMP'
         '''.format(vTable)
    cursor.execute(vSQL)
    if not cursor.fetchone():
        vSQL='''CREATE GLOBAL TEMPORARY TABLE {0}_TEMP AS
               (SELECT * FROM {1} WHERE 1=2)
             '''.format(vTable, vTable)
        cursor.execute(vSQL)
        print '%s_TEMP TEMPORARY TABLE HAS BEEN CREATED.' % vTable

    vKey=None          # Primary Key
    vSQL = '''SELECT cols.column_name
                 FROM all_constraints cons, all_cons_columns cols
                 WHERE cols.table_name = '{0}'
                 AND cons.constraint_type = 'P'
                 AND cons.constraint_name = cols.constraint_name
                 AND cols.owner='DBATEZ'
                 AND cons.owner = cols.owner
            '''.format(vTable)

    cursor.execute(vSQL)
    vKey = cursor.fetchone()[0]


    vSQL = '''
       SELECT COLUMN_NAME, DATA_PRECISION, DATA_SCALE, DATA_TYPE, NULLABLE
              FROM USER_TAB_COLUMNS
              WHERE TABLE_NAME='{0}' ORDER BY COLUMN_ID 
           '''.format(vTable)
    cursor.execute(vSQL)
    table_columns = cursor.fetchall()

    vSQL = mergeTable(vTable, vKey, table_columns)

    (sql_insert, L) = validFields(infile, vTable, table_columns)

    cursor.executemanyprepared(sql_insert)
    cursor.executemany(sql_insert, L)
    cursor.execute(vSQL)              # Merge
    db.commit()
    cursor.close()
    db.close()
