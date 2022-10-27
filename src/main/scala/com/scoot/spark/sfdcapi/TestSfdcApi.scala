package com.scoot.spark.sfdcapi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
// import org.scalatest.flatspec.AnyFlatSpec

object TestSfdcApi extends App{

    def fn(st: Row):Unit={
        println(st(0))
        println(st.getClass)
    }
    val spark = SparkSession.builder.master("local[1]").appName("sampleTest").getOrCreate()
    val sc = spark.sparkContext
    val schema = StructType(Array(StructField("value1",StringType,true)))
    val d = spark.read.format("sfdcapi").option("apiendpoint","https://httpbin.org/cookies/set?freeform=test").schema(schema).load()
    d.show()
        // assert(Seq("hello","how","are","you")===d.collect())

    // val testdata = Seq("hello","how","are","you")
    // val ds = sc.parallelize(testdata)
    // val rdd = ds.map(r=>Row.fromSeq(r))
    // println(rdd.getClass)
    // ds.collect.foreach(fn)
    // }
}