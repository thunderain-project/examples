Spark SimRank Algorithm Implementation
===

This package includs 5 different SimRank implementations: DFS (depth-first search) MapReduce, naive
MapReduce, delta MapReduce, matrix multiplication and PageRank-like Random Walk with Restart. You
can choose different implementation through configuration.

This implementation is compatible with Spark 0.8.1+ version, you can compile using `sbt assembly`,
before that please configure the correct Hadoop version in `build.sbt`.

How to Run
===

1. Using `graph_generate.py` to generate random adjacency matrix, you can configure `GRAPH_SIZE`
   (number of vertices), `EDGE_SIZE` (number of edges) to control the matrix rank, this script will
   serialize matrix to file.
2. Generate initial similarity matrix. Using `./run simrank.SimRankDataPrepare` to generate data, it
   should be noted that two parameters `graphASize` and `graphBSize`, which specifies the
   vertices number of two sub-graphs in the bipartite graph, should be the same as step 1's generated
   result.
3. Configure `config/config.properties` and run by `./run simrank.SimRankImpl`.

Notes
===

* Step 2 data preparation will generate one initial similarity matrix and one identity matrix,
  here similarity matrix is a upper triangluar matrix, implementation *delta MapReduce* will use this
  matrix as a initial input similarity matrix, for other implementations identity matrix would be
  enough to use as a initial input similarity matrix, you can skip step 2 if identity matrix is
  created by yourself.
* Here we focused on *matrix multiplication* implementation, other implementations are implemented
  only for reference, may not be well tuned.

---

This implementation is open sourced under Apache License Version 2.0.
