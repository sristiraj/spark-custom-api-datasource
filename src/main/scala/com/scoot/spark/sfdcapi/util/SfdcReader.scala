package com.scoot.spark.sfdcapi.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class SfdcReader{
    def reader(sqlContext:SQLContext, endpoint: String): RDD[String] = {
        val sess = requests.Session()
        val resp = sess.get(endpoint)
        val data = resp.text
        val sc = sqlContext.sparkContext
        val js = scala.util.parsing.json.JSON.parseFull(data)
        val testdata = Seq("hello","how","are","you")
        val ds = sc.parallelize(testdata)
        // val js = scala.util.parsing.json.JSON.parseFull(data)
        // val df = spark.sparkContext.parallelize(data)
        ds
    }
}
