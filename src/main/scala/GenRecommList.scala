import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object GenRecommList {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("输入参数不够")
      sys.exit(1)
    }
    val itemSimPath = args(0)
    val recommListPath = args(1)

    val conf = new SparkConf()
      .setAppName("gen_recomm_list")
      .set("spark.executor.memory", "20g")
      .set("spark.driver.memory", "10g")
      .set("spark.cores.max", "30")
      .set("spark.dynamicAllocation.enabled", "false")
      //                  .setMaster("local[10]")
      .setMaster("spark://qd01-tech2-spark001:7077,qd01-tech2-spark002:7077")
    val sc = new SparkContext(conf)

    val recomRDD = sc.textFile(itemSimPath).map(_.split("\t"))
      .filter(_.length==3).flatMap(x=>{
      val arr1 = ListBuffer[Tuple2[String, List[String]]]()
      val arr2 = ListBuffer[String]()
      arr2.append(x(0) + "|" + x(2))
      arr2.append(x(1) + "|" + x(2))
      arr1.append((x(0), arr2.toList))
      arr1.append((x(1), arr2.toList))
      for (i <- arr1)
        yield i
    }).reduceByKey(_:::_).map(x=>(x._1, x._2.toSet.toList.mkString("{]")))

    recomRDD.saveAsTextFile(recommListPath)
  }
}
