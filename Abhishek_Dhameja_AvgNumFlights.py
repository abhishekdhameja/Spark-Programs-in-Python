from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: AvgNumFlights.py <file>", file=sys.stderr)
        exit(-1)

    sc=SparkContext(appName='AvgNumFlights')

    lines=sc.textFile(sys.argv[1]).map(lambda s: s.encode("ascii", "ignore").split(','))\
            .map(lambda s: (s[1].split(' ')[0].split('/')[2]+', '+s[3],int(s[5])))\
            .aggregateByKey((0,0), lambda U,v: (U[0] + v, U[1] + 1), lambda U1,U2: (U1[0] + U2[0], U1[1] + U2[1]))\
            .map(lambda (x, (y, z)): (x, float(y)/z))

    output = lines.sortByKey(True).collect()
    for (key,value) in output:
        print("%s, %i" % (key,value))