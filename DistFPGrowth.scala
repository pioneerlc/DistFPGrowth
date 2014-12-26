/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * Created by clin3 on 2014/12/24.
 */

class DistFPGrowth (
    private var supportThreshold: Double,
    private var splitterPattern: String,
    private var numGroups: Long) extends Serializable {

  def this() = this(
    DistFPGrowth.DEFAULT_SUPPORT_THRESHOLD,
    DistFPGrowth.DEFAULT_SPLITTER_PATTERN,
    DistFPGrowth.DEFAULT_NUM_GROUPS
  )

  /** Set the support threshold. Support threshold must be defined on interval [0, 1]. Default: 0. */
  def setSupportThreshold(supportThreshold: Double): this.type = {
    if (supportThreshold < 0 || supportThreshold > 1) {
      throw new IllegalArgumentException("Support threshold must be defined on interval [0, 1]")
    }
    this.supportThreshold = supportThreshold
    this
  }

  /** Set the splitter pattern within which we can split transactions into items. */
  def setSplitterPattern(splitterPattern: String): this.type = {
    this.splitterPattern = splitterPattern
    this
  }

  /** Set the number of groups the features should be divided. */
  def setNumGroups(numGroups: Long): this.type  = {
    this.numGroups = numGroups
    this
  }

  /** Map each item to an unique id which starts from 0. */
  def getFMap(fList: Array[(String, Long)]) : HashMap[String, Long] = {
    var i = 0
    val fMap = new HashMap[String, Long]()
    for (pair <- fList) {
      fMap.put(pair._1, i)
      i += 1
    }
    fMap
  }

  /** Calculate the number of items per group. */
  def getNumPerGroup(fList: Array[(String, Long)], numGroups: Long): Long = {
    val numPerGroup = fList.length / numGroups
    if (fList.length % numGroups != 0) numPerGroup + 1 else numPerGroup
  }

  /** Compute the minimum support according to the support threshold. */
  def run(data: RDD[String]): RDD[(String, Long)] = {
    val minSupport = supportThreshold * data.count()
    run(data, minSupport)
  }

  /** Implementation of DistFPGrowth. */
  def run(data: RDD[String], minSupport: Double): RDD[(String, Long)] = {
    val sc = data.sparkContext

    // Split each line of data into items, flatMap transformation will transform
    // all items of all lines to one RDD[String]. Then we count the number of times every item
    // appears in data and it is the so-called support count.
    // For short, we just call it support. After that, we will filter fList
    // by minSupport and sort it by items' supports.
    val fList = data.flatMap(line => line.split(splitterPattern))
      .map(item => (item, 1L)).reduceByKey(_ + _).filter(_._2 >= minSupport)
      .sortBy(pair => pair._2, false)
    val localFList = fList.collect()

    // For the convenience of dividing all items in fList to numGroups groups,
    // we assign a unique ID which starts from 0 to every item in fList.
    val fMap = getFMap(localFList)
    val bcFMap = sc.broadcast(fMap)

    // Compute the number of items in each group.
    val numPerGroup = getNumPerGroup(localFList, numGroups)

    // DistFPGrowth contains two steps:
    // Step 1: Generate transactions for group-dependent database.
    // Step 2: Run classical FPGrowth on each group.
    val retRDD = data.map(record => record.split(splitterPattern))
      .flatMap(record => TransactionsGenerator().run(record, bcFMap, numPerGroup))
      .groupByKey.flatMap(record => FPGrowth().run(record, minSupport))
      .groupByKey().map(record => (record._1, record._2.max))

    // Add frequent 1-itemsets into retRDD.
    retRDD.++(fList)
  }

  object TransactionsGenerator {
    def apply(): TransactionsGenerator = {
      val generator = new TransactionsGenerator()
      generator
    }
  }

  class TransactionsGenerator() {
    /**
     * TransactionsGenerator performs the following two steps:
     * Step 1: For each item a, substitute a by corresponding group-id according bcFMap.
     * Step 2: For each group-id of bcFMap, if it appears in record, locate its
     * right-most appearance, and output all items in front of it.
     */
    def run(
        record: Array[String],
        bcFMap: Broadcast[HashMap[String, Long]],
        numPerGroup: Long): ArrayBuffer[(Long, ArrayBuffer[String])] = {

      // Compute the group-id of item according to its itemId.
      def getGroupId(itemId: Long, numPerGroup: Long): Long = {
        itemId / numPerGroup
      }

      val fMap = bcFMap.value

      var retArr = new ArrayBuffer[(Long, ArrayBuffer[String])]()
      var itemArr = new ArrayBuffer[Long]()
      val groups = new ArrayBuffer[Long]()

      for(item <- record) {
        if(fMap.keySet.contains(item)) {
          itemArr += fMap(item)
        }
      }

      itemArr = itemArr.sortWith(_ < _)

      for (i <- (0 until itemArr.length).reverse) {
        val itemId = itemArr(i)
        val groupId = getGroupId(itemId, numPerGroup)
        if (!groups.contains(groupId)) {
          val tempItems = new ArrayBuffer[Long]()
          tempItems ++= itemArr.slice(0, i + 1)
          val items = tempItems.map(x => fMap.map(_.swap).getOrElse(x, null))
          retArr += groupId -> items
          groups += groupId
        }
      }
      retArr
    }
  }

  object FPGrowth {
    def apply(): FPGrowth = {
      val fpgrowth = new FPGrowth()
      fpgrowth
    }
  }

  class FPGrowth() {
    /** Call classical FPGrowth on each group. */
    def run(
        line: (Long, Iterable[ArrayBuffer[String]]),
        minSupport: Double): ArrayBuffer[(String, Long)] = {
      val transactions = line._2
      val localFPTree = new LocalFPTree(new ArrayBuffer[(String, Long)])
      localFPTree.fpgrowth(transactions, new ArrayBuffer[String], minSupport)
      localFPTree.retArr
    }
  }

  class LocalFPTree(var retArr: ArrayBuffer[(String, Long)]) {

    /** Build header table. */
    def buildHeaderTable(
        data: Iterable[ArrayBuffer[String]],
        minSupport: Double): ArrayBuffer[TreeNode] = {
      if (data.nonEmpty) {
        val map: HashMap[String, TreeNode] = new HashMap[String, TreeNode]()
        for (record <- data) {
          for (item <- record) {
            if (!map.contains(item)) {
              val node: TreeNode = new TreeNode(item)
              node.count = 1
              map(item) = node
            } else {
              map(item).count += 1
            }
          }
        }
        val headerTable = new ArrayBuffer[TreeNode]()
        map.filter(_._2.count >= minSupport).values.toArray
          .sortWith(_.count > _.count).copyToBuffer(headerTable)
        headerTable
      } else {
        null
      }
    }

    /** Build local FPTree on each node. */
    def buildLocalFPTree(
        data: Iterable[ArrayBuffer[String]],
        headerTable: ArrayBuffer[TreeNode]): TreeNode = {
      val root: TreeNode = new TreeNode()
      for (record <- data) {
        val sortedTransaction = sortByHeaderTable(record, headerTable)
        var subTreeRoot: TreeNode = root
        var tmpRoot: TreeNode = null
        if (root.children.nonEmpty) {
          while (sortedTransaction.nonEmpty &&
            subTreeRoot.findChild(sortedTransaction.head.toString) != null) {
            tmpRoot = subTreeRoot.children.find(_.name.equals(sortedTransaction.head))
            match {
              case Some(node) => node
              case None => null
            }
            tmpRoot.count += 1
            subTreeRoot = tmpRoot
            sortedTransaction.remove(0)
          }
        }
        addNodes(subTreeRoot, sortedTransaction, headerTable)
      }

      /** Sort items in descending order of support. */
      def sortByHeaderTable(
          transaction: ArrayBuffer[String],
           headerTable: ArrayBuffer[TreeNode]): ArrayBuffer[String] = {
        val map: HashMap[String, Long] = new HashMap[String, Long]()
        for (item <- transaction) {
          for (index <- 0 until headerTable.length) {
            if (headerTable(index).name.equals(item)) {
              map(item) = index
            }
          }
        }

        val sortedTransaction: ArrayBuffer[String] = new ArrayBuffer[String]()
        map.toArray.sortWith(_._2 < _._2).foreach(sortedTransaction += _._1)
        sortedTransaction
      }

      /** Insert nodes into FPTree. */
      def addNodes(
          parent: TreeNode,
          transaction: ArrayBuffer[String],
          headerTable: ArrayBuffer[TreeNode]) {
        while (transaction.nonEmpty) {
          val name: String = transaction.head
          transaction.remove(0)
          val leaf: TreeNode = new TreeNode(name)
          leaf.count = 1
          leaf.parent = parent
          parent.children += leaf

          var temp = true
          var index: Int = 0

          while (temp && index < headerTable.length) {
            var node = headerTable(index)
            if (node.name.equals(name)) {
              while(node.nextHomonym != null)
                node = node.nextHomonym
              node.nextHomonym  = leaf
              temp = false
            }
            index += 1
          }

          addNodes(leaf, transaction, headerTable)
        }
      }

      root
    }

    /** Implementation of classical FPGrowth. Mining frequent itemsets from FPTree. */
    def fpgrowth(
        transactions: Iterable[ArrayBuffer[String]],
        prefix: ArrayBuffer[String],
        minSupport: Double) {
      val headerTable: ArrayBuffer[TreeNode] = buildHeaderTable(transactions, minSupport)

      val treeRoot = buildLocalFPTree(transactions, headerTable)

      if (treeRoot.children.nonEmpty) {
        if (prefix.nonEmpty) {
          for (node <- headerTable) {
            var tempStr: String = ""
            val tempArr = new ArrayBuffer[String]()
            tempArr += node.name
            for (pattern <- prefix) {
              tempArr += pattern.toString
            }
            tempStr += tempArr.sortWith(_ < _).mkString(" ").toString
            retArr += tempStr -> node.count
          }

        }

        for (node: TreeNode <- headerTable) {
          val newPostPattern: ArrayBuffer[String] = new ArrayBuffer[String]()
          newPostPattern += node.name
          if (prefix.nonEmpty)
            newPostPattern ++= prefix
          val newTransactions: ArrayBuffer[ArrayBuffer[String]] =
            new ArrayBuffer[ArrayBuffer[String]]()
          var backNode: TreeNode = node.nextHomonym
          while (backNode != null) {
            var counter: Long = backNode.count
            val preNodes: ArrayBuffer[String] = new ArrayBuffer[String]()
            var parent: TreeNode = backNode.parent
            while (parent.name != null) {
              preNodes += parent.name
              parent = parent.parent
            }
            while (counter > 0) {
              newTransactions += preNodes
              counter -= 1
            }
            backNode = backNode.nextHomonym
          }

          fpgrowth(newTransactions, newPostPattern, minSupport)
        }

      }
    }
  }
}

/**
 * Top-level methods for calling DistFPGrowth.
 */
object DistFPGrowth {

  // Default values.
  val DEFAULT_SUPPORT_THRESHOLD = 0
  val DEFAULT_SPLITTER_PATTERN = " "
  val DEFAULT_NUM_GROUPS = 128

  /**
   * Run DistFPGrowth using the given set of parameters.
   * @param data transactional dataset stored as `RDD[String]`
   * @param supportThreshold support threshold
   * @param splitterPattern splitter pattern
   * @param numGroups Number of groups the features should be divided.
   * @return frequent itemsets stored as `RDD[(String, Long)]`
   */
  def run(
      data: RDD[String],
      supportThreshold: Double,
      splitterPattern: String,
      numGroups: Int): RDD[(String, Long)] = {
    new DistFPGrowth()
      .setSupportThreshold(supportThreshold)
      .setSplitterPattern(splitterPattern)
      .setNumGroups(numGroups)
      .run(data)
  }

  def run(
      data: RDD[String],
      supportThreshold: Double,
      splitterPattern: String): RDD[(String, Long)] = {
    new DistFPGrowth()
      .setSupportThreshold(supportThreshold)
      .setSplitterPattern(splitterPattern).run(data)
  }

  def run(data: RDD[String], supportThreshold: Double): RDD[(String, Long)] = {
    new DistFPGrowth().setSupportThreshold(supportThreshold).run(data)
  }

  def run(data: RDD[String]): RDD[(String, Long)] = {
    new DistFPGrowth().run(data)
  }
}
