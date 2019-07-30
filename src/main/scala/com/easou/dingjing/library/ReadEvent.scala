/*************************************************************************
> FileName: com/easou/dingjing/ReadEvent.scala
> Author  : DingJing
> Mail    : dingjing@live.cn
> Created Time: 2018年04月17日 星期二 14时46分06秒
 ************************************************************************/
package com.easou.dingjing.library

import scala.collection.mutable.{ListBuffer, Map}
class ReadEvent() {
  def parseLine(line : String) : ReadEvent = {
    val arr = line.split("\\x01", -1)
    if (arr.length < 23) {
      return this
    }
    for (i <- 0 to 20) {
      val key = this.field(i.toString)
      this.field(key) = arr(i)
      this.field -= i.toString
    }
    val para = arr(21).split("\\x02", -1)
    for (i <- para) {
      val itemArr = i.split("\\x03", -1)
      if (itemArr.length > 1) {
        field(itemArr(0).trim) = itemArr(1)
      } else {
        field(itemArr(0).trim) = ""
      }
    }
    field("server_time") = arr(22)
    this
  }

  def getKeys(): Iterable[String] = {
    field.keys
  }

  def getValue(key: String) : String = {
    var value = ""
    try {
      value = field(key)
    } catch {
      case ex : Throwable =>
    }
    value
  }

  def getValues(keys : List[String]) : List[String] = {
    var res = ListBuffer[String]()
    if (keys.nonEmpty) {
      for (i <- keys) {
        res += getValue(i.trim())
      }
    }
    res.toList
  }
  private var field = Map("0" -> "os",
    "1" -> "phone_udid2",
    "2" -> "phone_softversion",
    "3" -> "last_cpid",
    "4" -> "package_name",
    "5" -> "appkey",
    "6" -> "sdk_version",
    "7" -> "cpid",
    "8" -> "currentnetworktype",
    "9" -> "type",
    "10" -> "phone_imei",
    "11" -> "phone_apn",
    "12" -> "phone_udid",
    "13" -> "gatewayip",
    "14" -> "phone_mac",
    "15" -> "phone_imsi",
    "16" -> "phone_city",
    "17" -> "src_code",
    "18" -> "status",
    "19" -> "time",
    "20" -> "event_id")
}
