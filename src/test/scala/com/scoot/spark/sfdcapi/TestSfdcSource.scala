package com.scoot.spark.sfdcapi

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class TestSfdcApi extends AnyFlatSpec{
    "spark app" should "create custom data source" in {
        val spark = SparkSession.builder.master("local[1]").appName("sampleTest").getOrCreate()
        val sc = spark.sparkContext
        val d = spark.read.format("sfdcapi").option("apiendpoint","https://httpbin.org/cookies/set?freeform=test").load()
        print(d.collect())
        assert(Seq("hello","how","are","you")===d.collect())
    }
}