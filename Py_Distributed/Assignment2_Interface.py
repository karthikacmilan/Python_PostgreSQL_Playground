#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2

frange = open('RangeQueryOut.txt','w')
fpoint = open('PointQueryOut.txt','w')
rngCount = 0;
rbnCount = 0;
def getPCount(openconnection):
    global rngCount,rbnCount
    maincursor=openconnection.cursor()
    maincursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    all_tables=maincursor.fetchall()
    for table in all_tables :
        if table[0].startswith("rangeratingspart"):
            rngCount+=1
        if table[0].startswith("roundrobinratingspart"):
            rbnCount+=1
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.


    global rngCount,rbnCount
    if(ratingMaxValue < ratingMinValue or ratingMaxValue < 0  or ratingMaxValue > 5 or  ratingMinValue < 0  or ratingMinValue > 5):

        print "Error"
        return
    else:
        cursor=openconnection.cursor()
        getPCount(openconnection)

        for i in range(0, rngCount):
            cursor.execute('SELECT * FROM rangeratingspart'+str(i))
            rows=cursor.fetchall()

            for eachrow in rows :
                if eachrow[2] >= ratingMinValue and eachrow[2] <=ratingMaxValue:
                    frange.write("RangeRatingsPart"+str(i)+", "+str(eachrow[0]) + ", "+ str(eachrow[1]) + ", "+ str(eachrow[2]) + " " + '\n')

        fetchUnion =""
        for i in range(0, rbnCount-2):
            fetchUnion = fetchUnion + 'SELECT * FROM RoundRobinRatingsPart'+str(i) + ' UNION '
        fetchUnion = fetchUnion + 'SELECT * FROM RoundRobinRatingsPart'+str(rbnCount-1)
        cursor.execute(fetchUnion)
        all_rows=cursor.fetchall()

        for rows in all_rows :
            if rows[2] >= ratingMinValue and rows[2] <=ratingMaxValue:
                frange.write("RoundRobinRatingsPart"+str(i)+", "+str(rows[0]) + ", "+ str(rows[1]) + ", "+ str(rows[2]) + " " + '\n')
        frange.close()

def PointQuery(ratingsTableName, ratingValue, openconnection):

    global rngCount,rbnCount
    if(ratingValue < 0  or ratingValue > 5):

        print "Error"
        return
    else:

        cur=openconnection.cursor()

        for i in range(0, rngCount):
            cur.execute('SELECT * FROM rangeratingspart'+str(i))
            rows=cur.fetchall()
            for eachrow in rows :
                 if rows[2] == ratingValue:
                    fpoint.write("RangeRatingsPart"+str(i)+", "+str(eachrow[0]) + ", "+ str(eachrow[1]) + ", "+ str(eachrow[2]) + " " + '\n')

        query =""

        for i in range(0, rbnCount):

                cur.execute('SELECT * FROM RoundRobinRatingsPart'+str(i))
                rows=cur.fetchall()
                for eachrow in rows :
                    if eachrow[2] == ratingValue:
                        fpoint.write("RoundRobinRatingsPart"+str(i)+", "+str(eachrow[0]) + ", "+ str(eachrow[1]) + ", "+ str(eachrow[2]) + " " + '\n')
        fpoint.close()