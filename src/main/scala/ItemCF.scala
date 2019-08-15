import java.util

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.spark_project.dmg.pmml.True
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.util.control._
import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object ItemCF {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("输入参数不够")
      sys.exit(1)
    }
    val lessPeople = 10                    // 30天内少于次数的书籍不计算
    val giduidPath = args(0)
    val gidmapPath = args(1)
    val gidRecomPath = args(2)

//    val giduidPath = "hdfs://10.26.26.145:8020/rs/dingjing/knn/2019-07-29/knn_30_gid_uid/"
//    val gidRecomPath = "hdfs://10.26.26.145:8020/rs/dingjing/knn/2019-07-29/item_recomm/"

    val conf = new SparkConf()
      .setAppName("knn_recomm")
      .set("spark.executor.memory", "20g")
      .set("spark.driver.memory", "10g")
      .set("spark.cores.max", "30")
      .set("spark.dynamicAllocation.enabled", "false")
//                  .setMaster("local[10]")
      .setMaster("spark://qd01-tech2-spark001:7077,qd01-tech2-spark002:7077")
    val sc = new SparkContext(conf)
    val log = Logger.getLogger("item knn")

    /* 获取 (gid, List(uid))列表 */
    val gidUidRDD = sc.textFile(giduidPath).map(x=>x.split("\t"))
      .filter(_.length >= 2).map(x=>(x(0), x(1).split("\\{\\]").toSet))
      .filter(_._2.size>=lessPeople).persist(StorageLevel.DISK_ONLY)

    /////////////////////////////////// 单机执行 /////////////////////////////////////////
//    val itemCount = gidUidRDD.count()
//    log.info("参与计算的物品数量：%d".format(itemCount))
//    /* 开始计算 */
//    val gidUidLocal = gidUidRDD.collect()
//    val arr = new ArrayBuffer[Tuple3[String, String, Double]]()
//    var it1 = 0
//    var index = 1
//    val arrlen = gidUidLocal.length
//
//    while (it1 < arrlen) {
//      val info1 = gidUidLocal(it1)
//      val gid1 = info1._1
//      val uid1 = info1._2
//      var it2 = it1 + 1
//      while (it2 < arrlen) {
//        val info2 = gidUidLocal(it2)
//        val gid2 = info2._1
//        val uid2 = info2._2
//        val sim = jaccard(uid1, uid2)
//        arr.append((gid1, gid2, sim))
//        it2 += 1
//      }
//      if (index % 100 == 0)
//      log.info("物品相似度计算 %d 完成！ 完成占比: %2.3f%%!".format(index, index.toFloat/itemCount * 100))
//      index += 1
//      it1 += 1
//    }
//    val jaccardRDD = sc.parallelize(arr).map(x => x._1 + "\t" + x._2 + "\t" + x._3.toString)
    ////////////////////////////////////  单机版结束  ///////////////////////////////////////////////


    ///////////////////////////////////  全局变量  //////////////////////////////////////////////////
//    val itemCount = gidUidRDD.count()
//    log.info("参与计算的物品数量：%d".format(itemCount))
    /* 开始计算 */
//    var gid1 = 0
//    var gid2 = 0
//    var index = 1
//    val gidUidLocal = gidUidRDD.map(_._1.toInt).collect()
//    var it1 = gidUidLocal.iterator
//    val arrlen = gidUidLocal.length
//    val arr = new collection.mutable.ListBuffer[Tuple2[Int, Int]]()
//
//    while (it1.hasNext) {
//      gid1 = it1.next()
//      val it2 = gidUidLocal.iterator
//      while (it2.hasNext) {
//        gid2 = it2.next()
//        if(gid2 > gid1) {
//          arr.append((gid1, gid2))
//        }
//      }
//      if (index % 100 == 0)
//        log.info("物品相似度对 %d 生成！ 生成占比: %2.3f%%!".format(index, index.toFloat/itemCount * 100))
//      index += 1
//    }
//    val gidUidG = sc.broadcast(gidUidRDD.map(x=>(x._1.toInt, x._2)).collectAsMap())
//    gidUidRDD.unpersist(true)
//    val jaccardRDD = sc.parallelize(arr,1000)
//      .map(x=>calc_sim(x._1, x._2, gidUidG.value))
//      .map(x => x._1 + "\t" + x._2 + "\t" + x._3.toString)
    //////////////////////////////////////////  全局变量结束  ///////////////////////////////////////////

    ////////////////////////////// 全局变量2 ////////////////////////////////////////////
    val itemCount = gidUidRDD.count()
    log.info("参与计算的物品数量：%d".format(itemCount))
    val gidUidListG = sc.broadcast(gidUidRDD.map(_._1.toInt).collect())
    val gidUidRDD1 = gidUidRDD.repartition(5000)
      .map(_._1.toInt).flatMap(x=>{
      val gid1 = x
      val it = gidUidListG.value.iterator
      val arr = ListBuffer[Tuple2[Int, Int]]()
      while (it.hasNext) {
        arr.append((gid1, it.next()))
      }
      for (i <- arr)
        yield i
    }).persist(StorageLevel.DISK_ONLY)
    val gidUidDictG = sc.broadcast(gidUidRDD.map(x=>(x._1.toInt, x._2)).collectAsMap())
    gidUidRDD.unpersist(true)
    val jaccardRDD = gidUidRDD1.map(x=>{
      val gid1 = x._1
      val gid2 = x._2
      var sim = 0.0
      if(gidUidDictG.value.contains(gid1) && gidUidDictG.value.contains(gid2)) {
        sim = jaccard(gidUidDictG.value(gid1), gidUidDictG.value(gid2))
      }
      (gid1, gid2, sim.toString)
    })
    ////////////////////////////////////////////////////////////////////////////////////

    /* 结果保存 */
    val gidMapG = sc.broadcast(
      sc.textFile(gidmapPath)
        .map(_.split("\t"))
        .map(x=>(x(0).toInt, x(1))).collectAsMap())
    jaccardRDD.map(x=>{
      val gid1 = x._1
      val gid2 = x._2
      var str = ""
      if(gidMapG.value.contains(gid1) && gidMapG.value.contains(gid2)) {
        str = gidMapG.value(gid1) + "\t" + gidMapG.value(gid2) + "\t" + x._3
      }
      str
    }).saveAsTextFile(gidRecomPath)
  }

  def calc_sim (x: Int, y: Int, dict: Map[Int, Set[String]]): Tuple3[String, String, Double] = {
    var sim = 0.0
    if (dict.contains(x) && dict.contains(y)) {
      sim = jaccard(dict(x), dict(y))
    }
    (x.toString, y.toString, sim)
  }
  def calc_sim (x: Tuple2[String, Set[String]], all: Array[Tuple2[String, Set[String]]]):
        Tuple2[String, Array[Tuple2[String,Double]]] = {
    val gid1 = x._1
    val uid1 = x._2
    var gid2 = ""
    var sim = 0.0
    val arr = ArrayBuffer[Tuple2[String, Double]]()

    val u2 = all.iterator
    while (u2.hasNext) {
      val it = u2.next()
      gid2 = it._1
      val uid2 = it._2
      sim = jaccard(uid1, uid2)
      arr.append((gid2, sim))
    }
    (gid1, arr.toArray)
  }

  def jaccard (x: Set[String], y: Set[String]): Double = {
    val z = (x & y).size.toDouble
    val m = (x ++ y).size.toDouble
    z / m
  }
}
