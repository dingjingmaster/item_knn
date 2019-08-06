import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat

import scala.collection.Map
import com.easou.dingjing.library.ReadEvent
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object DataDetail {
  def main(args: Array[String]): Unit = {
    if(args.length < 6) {
      println("请输入：")
    }
    val readeventPath = args(0)    // 用户阅读日志
    val todayStr = args(1)         // 今天的时间
    val readeventDay = args(2)     // 阅读日志天数
    val gidmapPath = args(3)
    val uidmapPath = args(4)
    val giduidPath = args(5)

//    val readeventPath = "hdfs://10.26.29.210:8020/user/hive/warehouse/event_info.db/b_read_chapter/ds="    // 用户阅读日志
//    val todayStr = "2019-07-30"         // 今天的时间
//    val readeventDay = 30     // 阅读日志天数
//    val giduidPath = ""       // 书籍{]用户阅读列表

    val conf = new SparkConf()
      .setAppName("knn_data")
      .set("spark.executor.memory", "20g")
      .set("spark.driver.memory", "6g")
      .set("spark.cores.max", "30")
      .set("spark.dynamicAllocation.enabled", "false")
//            .setMaster("local[10]")
      .setMaster("spark://qd01-tech2-spark001:7077,qd01-tech2-spark002:7077")
    val sc = new SparkContext(conf)
    // (uid/udid, gid)
    var readeventRDD = sc.parallelize(Seq[Tuple2[String, String]]())
    for (i <- get_path(readeventPath, todayStr, str_to_int(readeventDay))) {
      val dtrdd = sc.textFile(i).map(x=>{
        var gid0 = ""   // 书籍id
        var uid0 = ""   // 用户id

        val rd = new ReadEvent().parseLine(x)
          .getValues(List("uid", "appudid", "gid", "appid", "userarea"))
        val uid = rd.head
        val appudid = rd(1)
        val gid = rd(2)
        val appid = rd(3)
        val area = rd(4)

        // uid
        if("" != uid && "-1" != uid && "0" != uid) {
          uid0 = uid
        } else if("" != appudid && "0" != appudid && "-1" != appudid) {
          uid0 = appudid
        }

        // gid
        if("" != gid && "0" != gid && "-1" != gid) {
          gid0 = "i_" + gid
        }

        // 过滤暂时不管地区

        (uid0, gid0)
      }).filter(x=>x._1 != "" && x._2 != "")
      readeventRDD = readeventRDD.union(dtrdd)
    }
    readeventRDD.persist(StorageLevel.DISK_ONLY)
    // 输出 uid map
    val uidmapRDD = readeventRDD.map(x=>x._1).distinct().zipWithIndex().persist(StorageLevel.DISK_ONLY)
    val gidmapRDD = readeventRDD.map(x=>x._2).distinct().zipWithIndex().persist(StorageLevel.DISK_ONLY)

    uidmapRDD.repartition(1).map(x=>x._1 + "\t" + x._2).saveAsTextFile(uidmapPath)
    gidmapRDD.repartition(1).map(x=>x._1 + "\t" + x._2).saveAsTextFile(gidmapPath)

    val uidmapG = sc.broadcast(uidmapRDD.collectAsMap())
    val gidmapG = sc.broadcast(gidmapRDD.collectAsMap())

    readeventRDD.map(x=>uid_gid_map(x, uidmapG.value, gidmapG.value)).reduceByKey(_:::_)
      .filter(x=> x._1 != "" && x._2.nonEmpty).map(x => (x._1, x._2.toSet.toList))
      .map(x=>x._1 + "\t" + x._2.mkString("{]"))
      .repartition(1).saveAsTextFile(giduidPath)
  }

  def uid_gid_map(x: Tuple2[String, String], uidmap: Map[String, Long], gidmap: Map[String, Long]): Tuple2[String, List[String]] = {
    val gid = x._2
    var gid0 = ""
    val uid = x._1
    var uid0 = ""

    if (gidmap.contains(gid)) {
      gid0 = gidmap(gid).toString
    }
    if (uidmap.contains(uid)) {
      uid0 = uidmap(uid).toString
    }
    (gid0, List[String](uid0))
  }

  def str_to_int(str: String): Int = {
    var a: Int = 0
    try {
      a = str.toInt
    } catch {
      case _: Exception =>
    }
    a
  }

  def get_path(base: String, time: String, days: Int): List[String] = {
    var i = 0
    var tp = ""
    val arr = new ArrayBuffer[String]()
    val datastrparse = new SimpleDateFormat("yyyy-MM-dd")
    val dt = datastrparse.parse(time)
    val ca = Calendar.getInstance()
    ca.setTime(dt)
    for (i <- 0 until days) {
      tp = datastrparse.format(ca.getTime)
      arr.append(base + tp + "/")
      println(base + tp + "/")
      ca.add(Calendar.DAY_OF_MONTH, -1)
    }
    arr.toList
  }
}
