package com.dahua.tag

import com.dahua.tools.SNTools
import com.dahua.utils.RedisUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis
import ch.hsr.geohash.GeoHash

object BusinessTool {

  /**
   * 2将商圈经纬度使用GEOHash算法,写入到Redis
   */
  def main(args: Array[String]): Unit = {

    if(args.length!=1){
      println(
        """
          |缺少参数
          |inputPath
        """.stripMargin)
      sys.exit()
    }
    val Array(inputPath) = args

    val conf: SparkConf = new SparkConf().setAppName("BusinessTool").setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    //  读取parquet文件。
    spark.read.parquet(inputPath)
      // 获得lat  long
      .select("lat","longitude")
      .where("lat > 3 and lat < 54 and longitude > 73 and longitude < 136")
      .distinct()// 写入redis.考虑。 开启连接，关闭连接。
      .foreachPartition(itr=>{
        // 开启redis连接
        val jedis: Jedis = RedisUtil.getJedis
        itr.foreach(row=>{
          val lat: String = row.getAs[String]("lat")
          val longat: String = row.getAs[String]("longitude")
          // 调用百度API,获得的商圈信息
          val business: String = SNTools.getBusiness(lat+","+longat)
          if(business!=null && StringUtils.isNotEmpty(business) && business != ""){
            // GeoHash计算hash码                              39.9850750000,116.3161200000
            val code: String = GeoHash.withCharacterPrecision(lat.toDouble,longat.toDouble,8).toBase32
            jedis.set(code,business)
            println(business)
          }
        })
        jedis.close()
        // 关闭redis,连接
      })

    sc.stop()
    spark.stop()

  }
}
