package com.dahua.dim

import com.dahua.bean.Logbean
import com.dahua.utils.RedisUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object RedingAndRedis {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(
        """
          |inputpath
          |缺少参数
          |""".stripMargin)
      sys.exit()
    }

    //创建sparksession对象
    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .master("local[1]")
      .appName("ZoneDimForRDDV2")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    var Array(inputpath) = args
    val log: RDD[String] = sc.textFile(inputpath)
    //过滤出不为空的appid
    val logrdd: RDD[Logbean] = log.map(_.split(",", -1)).filter(_.length >= 85).map(Logbean(_)).filter(t => {
      !t.appid.isEmpty
    })

    //从redis中读取数据
    logrdd.map(log => {
      var appname: String = log.appname
      //如果appname等于空或者appname为空 就从redis中读取数据
      if (appname == "" || appname.isEmpty) {
        val jedis: Jedis = RedisUtil.getJedis
        if (jedis.get(log.appid) != null && jedis.get(log.appid).nonEmpty)
          appname = jedis.get(log.appid)
        else {
          (log.appid, "不明确")
        }
      }
      val ysqqs: List[Double] = DimZB.qqsRtp(log.requestmode, log.processnode)

      (appname, ysqqs)
    }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).foreach(println)

  }
}
