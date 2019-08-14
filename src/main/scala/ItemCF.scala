import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._

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

    /* 书籍数量 */
    val gidnumG = sc.broadcast(sc.textFile(gidmapPath).count())

    /* 获取 (gid, List(uid))列表 */
    val gidUidRDD = sc.textFile(giduidPath).map(x=>x.split("\t"))
      .filter(_.length >= 2).map(x=>(x(0).toInt, x(1).split("\\{\\]").toList))
      .filter(_._2.length>=lessPeople).persist(StorageLevel.DISK_ONLY)

    println("\n********************\n参与计算的物品数量：" + gidUidRDD.count() + "\n********************\n")

    /* 生成 (gid1|gid2, 用户列表) */
    val gidPairRDD = gidUidRDD.flatMap(x=>{
      val gid1 = x._1
      val uidInfo = x._2
      val buf = ArrayBuffer[Tuple2[String, List[String]]]()

      for (i <- 1 until gidnumG.value.toInt) {
        if(i > gid1) {
          buf.append((gid1.toString + "|" + i.toString, uidInfo))
        } else {
          buf.append((i.toString + "|" + gid1.toString, uidInfo))
        }
      }

      for (i <- buf.toList)
        yield i
    })

    val jaccardRDD = gidPairRDD.reduceByKey((x, y)=>sim_jaccard(x, y))

    /* 结果保存 */
    jaccardRDD.map(x=>x._1 + "\t" + x._2).repartition(1).saveAsTextFile(gidRecomPath)
  }

  def sim_jaccard(x: List[String], y:List[String]): List[String] = {
    val b = x.toSet ++ y.toSet
    val c = x.toSet & y.toSet
    var bn = 0.0
    var cn = c.size
    if (b.nonEmpty) bn = b.size
    ArrayBuffer[String]((bn / cn).toString).toList
  }

  def gid_vector(x: String, gidNum: Int): List[Tuple2[String, List[String]]] = {
    val arr = x.split("\\t")
    val buf = ArrayBuffer[Tuple2[String, List[String]]]()
    val gid = arr(0).toInt
    val info = arr(1).split("\\{\\]").toSet.toList
    val break = new Breaks
//    break.breakable{
    for (i <- 1 until gidNum) {
      val tmp = ArrayBuffer[Int]()
      tmp.append(gid)
      tmp.append(i)
      if (tmp.length == 2) {
        buf.append((tmp.sorted.mkString("|"), info))
      }
    }
//    }
    for (i <- buf.toList)
      yield i
  }

  /* 计算相似度 */
  def jaccard(x: Vector, y:Vector): Double = {
    var m = (x.toSparse.indices.toSet ++ y.toSparse.indices.toSet).size.toDouble
    val z = (x.toSparse.indices.toSet & y.toSparse.indices.toSet).size
    if (m <= 0) {
      m = 1
    }
    z / m
  }
}
