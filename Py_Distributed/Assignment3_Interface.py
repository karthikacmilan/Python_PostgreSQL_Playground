__author__ = 'KarthikMila'

#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import re
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'InputTable'
SECOND_TABLE_NAME = 'InputTable1'
SORT_COLUMN_NAME_FIRST_TABLE = 'rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'userid'
JOIN_COLUMN_NAME_FIRST_TABLE = 'rating'
JOIN_COLUMN_NAME_SECOND_TABLE = 'userid'
##########################################################################################################

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur=openconnection.cursor();
    cur.execute("drop table if exists "+ratingstablename+";")
    cur.execute("create table {0} (UserID int,MovieID int,Rating numeric)".format(ratingstablename))
    print 'Created table'
    with open(ratingsfilepath,'r') as flatfile:
     for line in flatfile:
        column=line.split('::')
        cur.execute("insert into "+ratingstablename + "(UserID, MovieID, Rating) values("+column[0]+","+column[1]+","+column[2]+")")
    openconnection.commit()
    cur.close()
    print "Loaded values in table"
    print ""

def rangePartition(ratingstablename, column1, openconnection):
    loadratings(ratingstablename,'C:\\Users\\KarthikMila\\Desktop\\ml-10M100K\\ratings1.dat', con)
    try:
        cur = openconnection.cursor()
        cur.execute("select min("+column1+") from "+ratingstablename)
        val1 = cur.fetchone()
        cur.execute("select max("+column1+") from "+ratingstablename)
        val2 = cur.fetchone()
        Min=val1[0]
        a=Min
        Max=val2[0]
        step=Min-Max
        x=step/5
        i=x
        for ra in range(0, 5):
            cur.execute("create table partition"+str(ra)+" (LIKE " +ratingstablename+ " including defaults)")
            cur.execute("insert into partition"+str(ra)+" select * from "+ratingstablename+" where "+column1+">="+str(a)+" and "+column1+"<"+str(a+i))
            ra += 1
            a=a+x
        ra -= 1
        cur.execute("insert into partition"+str(ra)+" select * from "+ratingstablename+" where "+column1+"="+str(Max))
        openconnection.commit()
        cur.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cur:
            cur.close()


def sort(tablename,SortingColumnName, openconnection):
    cur=openconnection.cursor()
    cur.execute("create table "+tablename+"x (LIKE "+tablename +")")
    cur.execute("insert into "+tablename+"x select * from "+tablename+" ORDER BY "+SortingColumnName)
    cur.close()

def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):


    rangePartition(InputTable,SortingColumnName,openconnection)
    thread1= threading.Thread(target=sort,args=("partition0",SortingColumnName,openconnection))
    thread2= threading.Thread(target=sort,args=("partition1",SortingColumnName,openconnection))
    thread3= threading.Thread(target=sort,args=("partition2",SortingColumnName,openconnection))
    thread4= threading.Thread(target=sort,args=("partition3",SortingColumnName,openconnection))
    thread5= threading.Thread(target=sort,args=("partition4",SortingColumnName,openconnection))

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()

    cur = openconnection.cursor()
    cur.execute("CREATE TABLE "+OutputTable+" (LIKE " +InputTable+ " including defaults)")
    cur.execute("INSERT INTO "+OutputTable+" SELECT * FROM partition0x")
    cur.execute("INSERT INTO "+OutputTable+ " SELECT * FROM partition1x")
    cur.execute("INSERT INTO "+OutputTable+ " SELECT * FROM partition2x")
    cur.execute("INSERT INTO "+OutputTable+ " SELECT * FROM partition3x")
    cur.execute("INSERT INTO "+OutputTable+ " SELECT * FROM partition4x")
    cur.close()

def rangePartitionac(ratingstablename, column1, openconnection):
    loadratings(ratingstablename1,'C:\\Users\\KarthikMila\\Desktop\\ml-10M100K\\ratings1.dat', con)
    try:
        cur = openconnection.cursor()
        cur.execute("select min("+column1+") from "+ratingstablename)
        val1 = cur.fetchone()
        cur.execute("select max("+column1+") from "+ratingstablename)
        val2 = cur.fetchone()
        Min=val1[0]
        a=Min
        Max=val2[0]
        step=Min-Max
        x=step/5
        i=x
        for ra in range(0, 5):
            cur.execute("create table partitionac"+str(ra)+" (LIKE " +ratingstablename1+ " including defaults)")
            cur.execute("insert into partitionac"+str(ra)+" select * from "+ratingstablename1+" where "+column1+">="+str(a)+" and "+column1+"<"+str(a+i))
            ra += 1
            a=a+x
        ra -= 1
        cur.execute("insert into partitionac"+str(ra)+" select * from "+ratingstablename1+" where "+column1+"="+str(Max))
        openconnection.commit()
        cur.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cur:
            cur.close()


def join (table1, table2, table1joinColumn, table2joinColumn, table1partition_no, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table1.lower() +"'")
    val1 = cur.fetchall()
    cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table2.lower() + "'")
    val2 = cur.fetchall()
    query = "CREATE TABLE joinpartition"+str(table1partition_no)
    cur.execute(result)
    cur.execute("select * from partitionac"+str(table1partition_no)+" t1, " + table2 + " t2 WHERE " +  "t1."+table1joinColumn + "=" +"t2."+table2joinColumn)
    val3 = cur.fetchall()
    cur.execute("DROP TABLE partitionac"+str(table1partition_no))
    cur.executemany("insert into joinpartition{} values ({})".format(str(table1partition_no), attributes), val3)
    cur.close()
    return


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

    rangePartition(InputTable1, Table1JoinColumn, openconnection)
    thread1 = threading.Thread(target=join, args=(InputTable, InputTable1, Table1JoinColumn, Table2JoinColumn, 1, openconnection))
    thread2 = threading.Thread(target=join, args=(InputTable, InputTable1, Table1JoinColumn, Table2JoinColumn, 2, openconnection))
    thread3 = threading.Thread(target=join, args=(InputTable, InputTable1, Table1JoinColumn, Table2JoinColumn, 3, openconnection))
    thread4 = threading.Thread(target=join, args=(InputTable, InputTable1, Table1JoinColumn, Table2JoinColumn, 4, openconnection))
    thread5 = threading.Thread(target=join, args=(InputTable, InputTable1, Table1JoinColumn, Table2JoinColumn, 5, openconnection))

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()

    cur = openconnection.cursor()
    cur.execute("SELECT * INTO "+OutputTable+" FROM joinpartition1")
    cur.execute("Delete from "+OutputTable)
    cur.execute("SELECT * INTO "+OutputTable1+" FROM joinpartition1")
    cur.execute("CREATE TABLE "+OutputTable12+" (LIKE " +joinpartition1+ " including defaults)")

    cur.execute("insert into "+OutputTable +" SELECT * FROM jpartition0x")
    cur.execute("insert into "+OutputTable +" SELECT * FROM jpartition1x")
    cur.execute("insert into "+OutputTable +" SELECT * FROM jpartition2x")
    cur.execute("insert into "+OutputTable +" SELECT * FROM jpartition3x")
    cur.execute("insert into "+OutputTable +" SELECT * FROM jpartition4x")

    cur.close()

def delete_table(table_name,openconnection):

    cur=openconnection.cursor()
    cur.execute("DROP table "+table_name)
    cur.close()

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
DATABASE_NAME = 'ddsassignment3'


def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
    # Creating Database ddsassignment2
    print "Creating Database named as ddsassignment2"
    createDB();

    # Getting connection to the database
    print "Getting connection from the ddsassignment2 database"
    con = getOpenConnection();

    # Calling ParallelSort
    print "Performing Parallel Sort"

    ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);
    ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelJoinOutputTable', con);
    # Calling ParallelJoin
    print "Performing Parallel Join"

    ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);

    # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
    saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
    saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

    # Deleting parallelSortOutputTable and parallelJoinOutputTable
    deleteTables('parallelSortOutputTable', con);
           deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
