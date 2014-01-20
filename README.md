Spark SimRank Algorithm Implementation
===

在这个算法包中实现了五种算法：深度遍历算法，naive MapReduce，delta MapReduce，matrix multiplication
和Random walk with restart的PageRank算法，通过配置中不同的配置可以选择使用不同的算法。

算法兼容的Spark版本是0.8.1及以上版本，编译和运行使用`sbt assembly`.

运行算法分为以下几个步骤：

1. 使用python脚本产生邻接矩阵，其中可以配置的是GRAPH_SIZE (邻接矩阵的顶点个数)，EDGE_SIZE (邻接矩阵
   中边的数量)，通过这个脚本会产生邻接矩阵的文件。
2. 产生初始相似矩阵，使用命令`./run simrank.SimRankDataPrepare`产生数据。需要注意的是这里的参数中有
   两个`graphASize`和`graphBSize`指的是二分图中两个子图的顶点数量，应和步骤1产生的结果保持一致。
3. 使用命令`./run simrank.SimRankImpl config/config.properties`运行SimRank算法。

Notes:

* 步骤2会产生初始的相似矩阵和一个单位阵，其中初始相似阵是一个上三角阵，算法delta MapReduce需要这样的
  上三角阵作为初始的输入相似矩阵进行计算。其他的算法如naive MapReduce, matrix multiplication和
  PageRank like算法可以使用单位阵作为初始输入相似矩阵，单位阵可以自己构造，这样就无需执行步骤2了。
* 五种算法中重点关注了marix multiplication的实现和调优，其他算法并没有进一步深究，只做参考。
* 深度遍历算法时间复杂度很高，只做实现参考。
