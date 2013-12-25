#!/usr/bin/python

import random
import numpy

GRAPHSET_SIZE = 100
EDGE_SIZE = 1400

SET_A_SIZE = random.randint(1, GRAPHSET_SIZE)
SET_B_SIZE = GRAPHSET_SIZE - SET_A_SIZE

print "set A size", SET_A_SIZE
print "set B size", SET_B_SIZE

# edges between A and B is zipfian distribution
#zipfians = numpy.random.zipf(2, EDGE_SIZE)
#normalized_zipf = (zipfians / float(max(zipfians))) * SET_A_SIZE

# uniform distribution
uniforms = numpy.random.uniform(0, 1.0, EDGE_SIZE)
normalized_uniform = uniforms * SET_A_SIZE
normalized_uniform.sort()


f = file("graph", "w")

for e in normalized_uniform:
    va = int(e)
    # randomly choose a vertex from set B
    vb = random.randint(SET_A_SIZE, GRAPHSET_SIZE - 1)
    value = 1.0

    # print "(%d, %d), %d" %(va, vb, value)
    f.write("((%d,%d),%f)\n" %(va, vb, value))
    f.write("((%d,%d),%f)\n" %(vb, va, value))

f.close()
