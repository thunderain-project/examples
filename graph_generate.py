#!/usr/bin/python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import random
import numpy

GRAPHSET_SIZE = 50
EDGE_SIZE = 700

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

d = {}
for e in normalized_uniform:
    va = int(e)
    if (d.has_key(va)):
        d[va] = d[va] + 1
    else:
        d[va] = 1

f = file("graph", "w")

for (k,v) in d.items():
    va = k
    # randomly choose a vertex from set B
    rands = set()
    for i in range(0, v):
        r = random.randint(SET_A_SIZE, GRAPHSET_SIZE - 1)
        rands.add(r)

    for r in rands:
        #print "(%d, %d), %d" %(va, r, 1.0)
        f.write("((%d,%d),%f)\n" %(va, r, 1.0))

f.close()
