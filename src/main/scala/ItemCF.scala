import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._
import org.spark_project.dmg.pmml.True
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.util.control._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

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
    val log = Logger.getLogger("org")

    /* 书籍数量 */
//    val gidnumG = sc.broadcast(sc.textFile(gidmapPath).count())

    /* 获取 (gid, List(uid))列表 */
    val gidUidRDD = sc.textFile(giduidPath).map(x=>x.split("\t"))
      .filter(_.length >= 2).map(x=>(x(0), x(1).split("\\{\\]").toSet))
      .filter(_._2.size>=lessPeople).persist(StorageLevel.DISK_ONLY)

    /* 参与计算的gid */
    val gidDictG = sc.broadcast(gidUidRDD.collect())
    val itemCount = gidUidRDD.count()
    log.info("参与计算的物品数量：%d".format(itemCount))

    /////////////////////////////////// 单机执行 /////////////////////////////////////////
    /* 单机执行相似度计算 */
    val gidUidLocal = gidUidRDD.collect()
    val arr = new ArrayBuffer[Tuple3[String, String, Double]]()
    val it1 = gidUidLocal.iterator
    var index = 1

    while (it1.hasNext) {
      val info1 = it1.next()
      val gid1 = info1._1
      val uid1 = info1._2
      val it2 = gidUidLocal.iterator
      while (it2.hasNext) {
        val info2 = it2.next()
        val gid2 = info2._1
        val uid2 = info2._2
        if (gid2.toInt > gid1.toInt) {
          val sim = jaccard(uid1, uid2)
          arr.append((gid1, gid2, sim))
        }
      }
      log.info("物品相似度计算 %s 完成！ 完成占比: %2.3f %%!".format(index, index.toFloat/itemCount * 100))
      index += 1
    }

    val jaccardRDD = sc.parallelize(arr).map(x => x._1 + "\t" + x._2 + "\t" + x._3.toString)
    /////////////////////////////////////////////////////////////////////////////////////
    /* 生成 (gid1|gid2, 用户列表) */
//    val gidPairRDD = gidUidRDD.flatMap(x=>{
//      val gid1 = x._1
//      val uidInfo = x._2
//      val buf = ArrayBuffer[Tuple2[String, List[String]]]()
//
//      if (gidDictG.value.contains(gid1)) {
//
//
//
//        for (i <- 1 until gidnumG.value.toInt) {
//          if(gidDictG.value.contains(i)) {
//            if(i > gid1) {
//              buf.append((gid1.toString + "|" + i.toString, uidInfo))
//            } else {
//              buf.append((i.toString + "|" + gid1.toString, uidInfo))
//            }
//          }
//        }
//      }
//
//      for (i <- buf.toList)
//        yield i
//    })
//
//    val jaccardRDD = gidPairRDD.reduceByKey((x, y)=>sim_jaccard(x, y))

    ////////////////////////////// broadcast ///////////////////////////////////////////
//    val jaccardRDD = gidUidRDD.map(x=>calc_sim(x, gidDictG.value)).map(x=>x._1 + "\t" + x._2.mkString("{]"))
    ////////////////////////////////////////////////////////////////////////////////////

    /* 结果保存 */
    jaccardRDD.repartition(1).saveAsTextFile(gidRecomPath)
  }

//  def sim_jaccard(x: List[String], y:List[String]): List[String] = {
//    val b = x.toSet ++ y.toSet
//    val c = x.toSet & y.toSet
//    var bn = 0.0
//    val cn = c.size
//    if (b.nonEmpty) bn = b.size.toFloat
//    ArrayBuffer[String]((bn / cn).toString).toList
//  }
//
//  def gid_vector(x: String, gidNum: Int): List[Tuple2[String, List[String]]] = {
//    val arr = x.split("\\t")
//    val buf = ArrayBuffer[Tuple2[String, List[String]]]()
//    val gid = arr(0).toInt
//    val info = arr(1).split("\\{\\]").toSet.toList
//    val break = new Breaks
////    break.breakable{
//    for (i <- 1 until gidNum) {
//      val tmp = ArrayBuffer[Int]()
//      tmp.append(gid)
//      tmp.append(i)
//      if (tmp.length == 2) {
//        buf.append((tmp.sorted.mkString("|"), info))
//      }
//    }
////    }
//    for (i <- buf.toList)
//      yield i
//  }
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
