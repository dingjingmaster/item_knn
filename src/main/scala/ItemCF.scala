import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg._

import scala.util.control._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object ItemCF {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("输入参数不够")
      sys.exit(1)
    }
    val lessPeople = 10                    // 30天内少于次数的书籍不计算
    val giduidPath = args(0)
    val gidmapPath = args(1)
    val uidmapPath = args(2)
    val gidRecomPath = args(3)

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

    /* 用户数量 */
//    val uidnumG = sc.broadcast(sc.textFile(uidmapPath).count())

    /* 书籍数量 */
    val gidnumG = sc.broadcast(sc.textFile(gidmapPath).count())

    /* 生成 (gid1|gid2, 用户列表) */
    val gidVectorRDD = sc.textFile(giduidPath).flatMap(x=>gid_vector(x, gidnumG.value.toInt)).persist(StorageLevel.DISK_ONLY)

    /* 开始计算相似度 */
    val jaccardRDD = gidVectorRDD.reduceByKey((x, y)=>sim_jaccard(x, y))

    /* 结果保存 */
    jaccardRDD.map(x=>x._1 + "\t" + x._2).repartition(1).saveAsTextFile(gidRecomPath)
//    val simResult = gidVectorRDD.map(_._1).flatMap(x=>calc_sim(x, gidnumG.value.toInt, gidVectorG.value)).filter(x=>x._1 != 0 && x._2 != 0)
//      .map(x => x._1.toString + "\t" + x._2.toString + "\t" + x._3.toString).repartition(1).saveAsTextFile(gidRecomPath)
//    val gidmapG = sc.broadcast(gidmapRDD.collectAsMap())
//
//    gidsimRDD.map(x => save_result(x, gidmapG.value)).filter(_!="").repartition(1).saveAsTextFile(gidRecomPath)
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

//  def save_result(x: Tuple2[String, Array[Tuple2[String, Double]]], map: Map[String, String]): String = {
//    var gidx = x._1
//    val infoy = x._2
//    var buf = ""`
//    var gidy = ""
//    var simy = 0.0
//    if (map.contains(gidx)) {
//      gidx = map(gidx)
//    } else {
//      return ""
//    }
//    for (i <- infoy) {
//      if(map.contains(i._1)) {
//        gidy = map(i._1)
//      } else {
//        gidy = ""
//      }
//      if ("" != buf && "" != gidy) {
//        buf += "{]" + gidy + "|" + i._2.toString
//      } else if ("" == buf && "" != gidy) {
//        buf = gidy + "|" + i._2.toString
//      }
//    }
//    gidx + "\t" + buf
//  }

  /* 计算相似度 */
  def jaccard(x: Vector, y:Vector): Double = {
    var m = (x.toSparse.indices.toSet ++ y.toSparse.indices.toSet).size.toDouble
    val z = (x.toSparse.indices.toSet & y.toSparse.indices.toSet).size
    if (m <= 0) {
      m = 1
    }
    z / m
  }

  def calc_sim(x: Int, max: Int, map: Map[Int, Vector]): List[Tuple3[Int, Int, Double]] = {
    val arr = ArrayBuffer[Tuple3[Int, Int, Double]]()
    val gidx:Int = x
    var gidy:Int = 0
    val loop = new Breaks
    var sim:Double = 0.0

    if (!map.contains(gidx)) {
      arr.append((0, 0, 0.0))
      return arr.toList
    }
    val valx = map(gidx)
    for (i <- x + 1 to max) {
      if (map.contains(i)) {
        gidy = i
        val valy = map(i)
        sim = jaccard(valx, valy)
        arr.append((gidx, gidy, sim))
      }
    }
    for(y <- arr.toList)
      yield y
  }
}
