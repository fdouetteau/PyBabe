#!/usr/bin/env PYTHONPATH=.. ../env/bin/python 

from pybabe import Babe
import re,sys

def wordcount():
    a = Babe().pull(protocol='http',host='www.ietf.org', filename='rfc/rfc1149.txt')
    a = a.flatMap(lambda row: [(w, 1) for w in re.findall('\w+', row.text)], columns=['word', 'count'])
    a = a.groupBy(key='word', reducer=lambda word, rows : (word, sum([row.count for row in rows])))
    a = a.maxN(column='count', n=10)
    a.push(stream=sys.stdout, format='csv')
    
if __name__ == "__main__":
    wordcount()
    