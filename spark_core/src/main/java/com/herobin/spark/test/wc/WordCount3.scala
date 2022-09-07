package com.herobin.spark.test.wc

import org.apache.spark.{SparkConf, SparkContext}

object WordCount3 {

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

    // Spark 框架提供了更多的功能，可以将分组和聚合使用一个方法实现
    // reduceByKey：相同的key的数据，可以对value进行reduce聚合
    // wordToOne.reduceByKey((x,y) => x + y) 效果同下
    var wordToCount = wordToOne.reduceByKey(_ + _)

    var array = wordToCount.collect()
    array.foreach(println)

    sc.stop()
  }

}
