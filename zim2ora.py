#!/usr/bin/env python
#-*- encoding: utf-8 -*-
# Exporta Dados de Arquivos CSV Para o Oracle
# * Campos de Data Devem ser validados antes de ser importados.

import cx_Oracle as dbatez
import sys, csv
from decimal import Decimal
from datetime import datetime

def validFields(inFile, table_columns):
    L = []
    #fields = \
    #[('id','NUMBER',6,0), ('salary','NUMBER',11,2), ('empdate','DATE','','')]
    #fileSales='contrato.csv' 
    with open(inFile, 'rb') as f:
       reader = csv.reader(f, delimiter=';', quotechar='"')
       for row in reader:
           print row
           for value in enumerate(row):
               pos = value[0]
               if table_columns[pos][3] == 'DATE' and \
                       value[1] not in (None, 0, '0'):
                   try:
                       datetime(value[1])
                   except TypeError:
                       row[pos] = 'null'
                   print 'Opa! Field DATE', value[1]
               elif table_columns[pos][3] == 'DATE' and \
                     value[1] in (None,0,'0'):
                         row[pos] = 'null'
                         print 'Opa! Field DATE', value[1]
           print '\n'
           print row
           L.append(tuple(row))

"""

sql_insert = ('INSERT INTO {0}_TEMP (%s) VALUES (%s)'.format(vTable) %
   (','.join('%s' % name for name in table_columns),
   ','.join("TO_NUMBER(:{0},'99999999.99')".format(conta + 1) if str(coluna[conta][1]).isdigit() else 
     "TO_DATE(:{0})".format(conta + 1) if str(coluna[conta][3])=='DATE' else
     ':{0}'.format(conta + 1) for conta in range(len(table_columns)))))


reader = csv.reader(open(infile), delimiter=';', quotechar='"')
for row in reader:
	L.append(tuple(row))
"""

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
    for column in table_columns:     # INSERT
        conta += 1
        if conta <> totalFields:
            vSQL += '     %s ,\n ' % (column[0])
        else:
            vSQL += '     %s  ) '  % (column[0])

    conta = 0
    vSQL += ''' VALUES ( '''
    for column in table_columns:     # INSERT
        conta += 1
        if conta <> totalFields:
            vSQL += '     TAB_B.%s,\n ' % (column[0])
        else:
            vSQL += '     TAB_B.%s ) '  % (column[0])

    return vSQL



vTable = sys.argv[1]  # Table Name in Oracle
infile = sys.argv[2]  # Input file CSV

db = dbatez.connect('user/pass@10.0.0.1/orcl')
cursor = db.cursor()

#--------------------------------- Check Table if exists  
vSQL='''SELECT  TABLE_NAME FROM USER_TABLES
                           WHERE TABLE_NAME = '{0}_TEMP'
     '''.format(vTable)
cursor.execute(vSQL)
if bool(cursor.fetchone()):
	pass
else:
	vSQL='''CREATE GLOBAL TEMPORARY TABLE {0}_TEMP AS
               (SELECT * FROM {1} WHERE 1=2)
             '''.format(vTable, vTable)
        cursor.execute(vSQL)

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
       SELECT COLUMN_NAME, DATA_PRECISION, DATA_SCALE, DATA_TYPE
              FROM USER_TAB_COLUMNS
              WHERE TABLE_NAME='{0}' ORDER BY COLUMN_ID 
       '''.format(vTable)
cursor.execute(vSQL)
table_columns = cursor.fetchall()

vSQL = mergeTable(vTable, vKey, table_columns)

validFields(infile, table_columns)

cursor.close()
db.close()
sys.exit(1)

L = []

sql_insert = ('INSERT INTO {0}_TEMP (%s) VALUES (%s)'.format(vTable) %
   (','.join('%s' % name for name in table_columns),
   ','.join("TO_NUMBER(:{0},'99999999.99')".format(conta + 1) if str(coluna[conta][1]).isdigit() else 
     "TO_DATE(:{0})".format(conta + 1) if str(coluna[conta][3])=='DATE' else
     ':{0}'.format(conta + 1) for conta in range(len(table_columns)))))


reader = csv.reader(open(infile), delimiter=';', quotechar='"')
for row in reader:
	L.append(tuple(row))

cursor.prepare(sql_insert)
cursor.executemany(sql_insert, L)
cursor.execute(vSQL)
db.commit()
cursor.close()
db.close()
