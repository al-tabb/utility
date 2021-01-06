import sys, os, psycopg2, csv
def main():
    dirname = os.path.dirname(__file__)
    csvPath = os.path.join(dirname, 'csv')
    textPath = os.path.join(dirname, 'copyTables.txt')
    sourceDatabase = "xxx"
    targetDatabase = "xxx"
    host = "xxx"
    port = "xxx"
    user = "xxx"
    password = "xxx"
    sourceConn = psycopg2.connect("dbname="+sourceDatabase+" user="+user+" password="+password+" host="+host+" port="+port)
    sourceCur = sourceConn.cursor()
    targetConn = psycopg2.connect("dbname="+targetDatabase+" user="+user+" password="+password+" host="+host+" port="+port)
    targetCur = targetConn.cursor()
    file1 = open(textPath, 'r') 
    Lines = file1.readlines()   
    count = 0
    csvFiles = []
    for line in Lines:        
        curLine = line.strip('\n')
        curSourceSchema = curLine.split(".")[0]
        curSourceTable = curLine.split(".")[1].strip('"')
        curSourceFullTable = curLine
        curCsvPath = os.path.join(csvPath, curSourceFullTable+".csv")
        csvFiles.append(curCsvPath)
        file2 = open(curCsvPath, 'w')
        tableColumns = getColumns(sourceCur, curSourceSchema, curSourceTable)
        csvHeader =""
        csvHeaderCount = 0
        sql = "SELECT * FROM " + curSourceFullTable
        sourceCur.execute(sql)
        curSourceRows = sourceCur.fetchall()        
        with open(curCsvPath, 'w', newline='') as csvFile:
            w = csv.writer(csvFile, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')
            w.writerow(tableColumns)
            for row in curSourceRows:
                w.writerow(row)
        #close csv file
        csvFile.close()
        primaryKey = getPrimaryKey(sourceCur, curSourceSchema, curSourceTable)
        columnsMinusPrimaryKey = removeFromList(tableColumns, primaryKey)
        with open(curCsvPath, 'r', newline='') as readCsvFile:
            r = csv.DictReader(readCsvFile, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')            
            for key in r:
                params = []
                conflictParams = []
                keyString = "INSERT INTO " + curSourceFullTable + " ("
                valueString = ") VALUES ("
                conflictString = ') ON CONFLICT ("' + primaryKey + '") DO UPDATE SET '
                keyCount = 0
                conflictCount = 0
                curPrimaryKey = -1
                for k in key:
                    if k!=primaryKey:
                        if conflictCount>0:
                            conflictString+=","
                        if key[k]=='':
                            conflictParams.append(None)
                        else:
                            conflictParams.append(key[k])
                        conflictString+= '"' + k + '"' + "=" + '%s'
                        conflictCount+=1
                    else:
                        curPrimaryKey = key[k]                        
                    if keyCount>0:
                        keyString+=","
                        valueString+=","
                    keyString += '"' + k + '"'
                    if key[k]=='':
                        params.append(None)
                    else:
                        params.append(key[k])
                    valueString+='%s'
                    keyCount+=1
                conflictString+= ' WHERE ' + curSourceFullTable + '."' + primaryKey + '"=' + "'" + curPrimaryKey + "'"
                sqlString = keyString + valueString + conflictString
                fullParams = params + conflictParams
                #execute sqlString on target database
                targetCur.execute(sqlString, fullParams)            
    targetConn.commit()
    targetCur.close()
    targetConn.close()
    sourceCur.close()
    sourceCur.close()
    
def getColumns(cur, schemaName, tableName):
    sql = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '" + tableName + "' AND table_schema = '" + schemaName + "'"
    cur.execute(sql)
    columnNames = cur.fetchall()
    returnColumns = []
    for c in columnNames:
        curColumn = c[0]
        returnColumns.append(curColumn)
    return returnColumns

def getPrimaryKey(cur, schemaName, tableName):
    sql = """SELECT c.column_name
FROM information_schema.table_constraints tc 
JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
  AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
  WHERE tc.table_schema = %s AND tc.table_name = %s AND tc.constraint_type = %s"""
    parameters = [schemaName, tableName, "PRIMARY KEY"]
    cur.execute(sql, parameters)
    result = cur.fetchall()
    return result[0][0]

def removeFromList(list, item):
    newList = []
    for l in list:
        if l != item:
            newList.append(l)
    return newList


if __name__ == "__main__":
    main()
