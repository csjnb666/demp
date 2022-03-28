package com.dahua.tag

import com.dahua.utils.TagUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.UUID

object TagRPT {

  def main(args: Array[String]): Unit = {
    if (args.length!=4){
      println(
        """
          |缺少参数
          |inputpath ,appMapping,stopwords,outputpath
          |""".stripMargin)
      sys.exit()
    }
    //创建sparksession对象
    var conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .appName("ProCityAnalyesForRDD")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    //接收参数
    var Array(inputpath,app_mapping,stopworks,outputpath) =args

    //读取app_mapping广播变量
    val map: Map[String, String] = sc.textFile(app_mapping).map(line => {
      val str: Array[String] = line.split("[:]", -1)
      (str(0), str(1))
    }).collect().toMap

    //广播app变量
    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(map)

    //停用词广播变量
    val stopworkmap: Map[String, Int] = sc.textFile(stopworks).map((_, 0)).collect().toMap
    val broadcaststopworkmap: Broadcast[Map[String, Int]] = sc.broadcast(stopworkmap)

    //读取数据源 打数据标签
    val df: DataFrame = spark.read.parquet(inputpath)
    val ds: Dataset[(String, List[(String, Int)])] = df.where(TagUtils.tagUserIdFilterParam).map(row => {
      // 广告标签
      val adsMap: Map[String, Int] = AgsTags.makeTags(row)
      // app标签.
      val appMap: Map[String, Int] = AppTags.makeTags(row, broadcast.value)
      // 驱动标签
      val driverMap: Map[String, Int] = DriverTags.makeTags(row)
      // 关键字标签
      val keyMap: Map[String, Int] = KeyTags.makeTags(row, broadcaststopworkmap.value)
      // 地域标签
      val pcMap: Map[String, Int] = PcTags.makeTags(row)


      //获取用户id
      if (TagUtils.getUserId(row).size > 0) {
        (TagUtils.getUserId(row)(0), (adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap).toList)
      } else {
        (UUID.randomUUID().toString.substring(0, 6), (adsMap ++ appMap ++ driverMap ++ keyMap ++ pcMap).toList)
      }
    })
    ds.rdd.reduceByKey((list1,list2)=>{
      (list1++list2).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList
    }).saveAsTextFile(outputpath)


  }
}
