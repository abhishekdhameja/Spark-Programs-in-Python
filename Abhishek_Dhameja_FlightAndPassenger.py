from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: FlightAndPassenger.py <file1> <file2>", file=sys.stderr)
        exit(-1)

    sc=SparkContext(appName='AvgNumFlights')

    lines1=sc.textFile(sys.argv[1]).map(lambda s: s.encode("ascii", "ignore").split(','))\
            .map(lambda s: (s[1].split(' ')[0].split('/')[0]+'/'+s[1].split(' ')[0].split('/')[2]+', '+s[3]+', '+s[4],int(s[5])))\
            .reduceByKey(add)

    lines2 = sc.textFile(sys.argv[2]).map(lambda s: s.encode("ascii", "ignore").split(',')) \
        .map(lambda s: (s[1].split(' ')[0].split('/')[0] + '/' + s[1].split(' ')[0].split('/')[2] + ', ' + s[3] + ', ' + s[4], int(s[5]))) \
        .reduceByKey(add)

    lines=lines1.join(lines2)

    output = lines.sortByKey(True).collect()
    for (key,values) in output:
        print("%s, %i, %i" % (key,values[0],values[1]))