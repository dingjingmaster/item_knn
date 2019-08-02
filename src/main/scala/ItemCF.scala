import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object ItemCF {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("输入参数不够")
      sys.exit(1)
    }
    val lessPeople = 100                  // 30天内少于次数的书籍不计算
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
    val gidmapRDD = sc.textFile(gidmapPath).map(x=>{
      var ret = ("", "")
      val arr = x.split("\t")
      if (arr.length >= 2) {
        ret = (arr(1), arr(0))
      }
      ret
    }).filter(_._1 != "")
    val giduidRDD = sc.textFile(giduidPath).map(x => {
      val arr = x.split("\\t")
      val gid = arr(0)
      val info = arr(1).split("\\{\\]")
      (gid, info.toSet)
    }).filter(_._2.size > lessPeople).persist(StorageLevel.DISK_ONLY)
    val gidudidG = sc.broadcast(giduidRDD.collect())
    val gidsimRDD = giduidRDD.map(x => calc_sim(x, gidudidG.value)).filter(_._2.nonEmpty)
//    gidudidG.destroy()
    val gidmapG = sc.broadcast(gidmapRDD.collectAsMap())

    gidsimRDD.map(x => save_result(x, gidmapG.value)).filter(_!="").repartition(1).saveAsTextFile(gidRecomPath)
  }

  def save_result(x: Tuple2[String, Array[Tuple2[String, Double]]], map: Map[String, String]): String = {
    var gidx = x._1
    val infoy = x._2
    var buf = ""
    var gidy = ""
    var simy = 0.0
    if (map.contains(gidx)) {
      gidx = map(gidx)
    } else {
      return ""
    }
    for (i <- infoy) {
      if(map.contains(i._1)) {
        gidy = map(i._1)
      } else {
        gidy = ""
      }
      if ("" != buf && "" != gidy) {
        buf += "{]" + gidy + "|" + i._2.toString
      } else if ("" == buf && "" != gidy) {
        buf = gidy + "|" + i._2.toString
      }
    }
    gidx + "\t" + buf
  }

  def calc_sim(x: Tuple2[String, Set[String]], array: Array[Tuple2[String, Set[String]]]):
      Tuple2[String, Array[Tuple2[String, Double]]] = {
    val gidx = x._1
    val ux = x._2
    var gidy = ""
    var sim = 0.0
    var arraybuf = ArrayBuffer[Tuple2[String, Double]]()
    var uy = Set[String]()
    var m = 0.0
    var z = 0.0
    val loop = new Breaks

    loop.breakable{
      for (info <- array) {
        gidy = info._1
        uy = info._2
        if (gidx != gidy) {
          m = (ux ++ uy).size
          z = (ux & uy).size
          if (m <= 0) {
            m = 1
          }
          sim = z / m
          if (sim >= 0.0001) {
            arraybuf.append((gidy, sim))
          }
        }
      }
    }
    if (arraybuf.nonEmpty) {
      arraybuf = arraybuf.sortBy(x=>x._2).reverse
    }
    (gidx, arraybuf.toArray)
  }
}
