package com.herobin.spark.test.wc

import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {

  def main(args: Array[String]): Unit = {

    // 建立和Spark框架的连接
    var sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    var sc = new SparkContext(sparkConf)

    // 1. 读取文件，获取一行一行的数据
    val lines = sc.textFile("datas")

    // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
    val words = lines.flatMap(_.split(" "))

    // 3. 将数据转换为tuple 后 根据单词进行分组，再进行reduce统计
    var wordToOne = words.map(word => (word, 1))
    var wordGroup = wordToOne.groupBy(word => word._1)
    var wordToCount = wordGroup.map {
      case (word, list) => {
        list.reduce((t1, t2) => (t1._1, t1._2 + t2._2))
      }
    }

    var array = wordToCount.collect()
    array.foreach(println)

    sc.stop()
  }

}
