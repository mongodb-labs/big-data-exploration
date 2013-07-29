#! /usr/bin/python

"""
pigpagerank.py -->
Embedded pig script to 
compute the pagerank of the graph stored in params['docs_in'].
Outputs the result to params['docs_out']. 

Modified from: http://techblug.wordpress.com/2011/07/29/pagerank-implementation-in-pig/
"""


from org.apache.pig.scripting import *

with open("oneiteration.pig") as f:
    P = Pig.compile(f.read());

params = {'d': '0.85', 'docs_in' : 'mongodb://localhost/test.pagerank_data_sample2'}

# max iterations is 100
for i in range(100):
    out = 'mongodb://localhost/test.pagerank_data_' + str(i+1)
    params['max_diff'] = "diff_store_" + str(i+1)
    params['docs_out'] = out
    stats = P.bind(params).runSingle()
    if not stats.isSuccessful():
        raise 'failed'
    
    max_diff_value = float(str(stats.result('max_diff').iterator().next().get(0)))
    print "max_diff_value = " + str(max_diff_value)
    if max_diff_value < 0.01:
        print "done at iteration " + str(i)
        break
    
    params['docs_in'] = out
