package com.dahua.dim

import com.dahua.bean.Logbean
import com.dahua.utils.RedisUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object RedingAndRedis {
  /**
   * 从redis中读取数据
   * @param args
   */
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
        //如果appid不为空 则取出来 反之则不明确
        if (jedis.get(log.appid) != null && jedis.get(log.appid).nonEmpty)
          appname = jedis.get(log.appid)
        else {
          (log.appid, "不明确")
        }
      }
      //原始请求数
      val ysqqs: List[Double] = DimZB.qqsRtp(log.requestmode, log.processnode)

      (appname, ysqqs)
    }).reduceByKey((list1, list2) => {
      //拉链 两个集合的合并操作，合并后每个元素是一个 对偶元组。
      //    val list1 = List(1, 2, 3)
      //    val list2 = List(4, 5, 6)
      //    val list3 = list1.zip(list2) // (1,4),(2,5),(3,6)
      //    println("list3=" + list3)
      //    for (item <- list3) {
      //      print(item._1 + " " + item._2) //取出时，按照元组的方式取出即可
      //    }
      list1.zip(list2).map(t => t._1 + t._2)
    }).foreach(println)

  }
}
