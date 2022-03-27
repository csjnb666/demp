package com.dahua.dim


import com.dahua.utils.RedisUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object AppMappingToRedis {

  def main(args: Array[String]): Unit = {
    if (args.length != 1){
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

    import  spark.implicits._

    var Array(inputpath) =args
    sc.textFile(inputpath).map(line=>{
      val arr: Array[String] = line.split("[:]", -1)
      (arr(0),arr(1))

    }).foreachPartition(ite=>{
      //连接redis
      val jedis: Jedis = RedisUtil.getJedis

      ite.foreach(mapping=>{
        //向redis中存储数据
        jedis.set(mapping._1,mapping._2)
      })
      jedis.close()
    })
  }
}
