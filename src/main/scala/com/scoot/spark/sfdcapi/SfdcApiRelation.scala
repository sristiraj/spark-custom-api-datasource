package com.scoot.spark.sfdcapi

import java.io.IOException
import java.text.SimpleDateFormat

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

import org.apache.commons.csv._
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types._
import com.scoot.spark.sfdcapi.util._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

case class SfdcApiRelation protected[spark] (
    baseRDD: (SQLContext, String) => RDD[String],
    endpoint: String,
    parseMode: String,
    schemaInput: StructType,
    inferSchemaFlag: Boolean,
    dateFormat: String)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Serializable {

  // Share date format object as it is expensive to parse date pattern.
  private val dateFormatter = if (dateFormat != null) new SimpleDateFormat(dateFormat) else null

  private val logger = LoggerFactory.getLogger(SfdcApiRelation.getClass)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  private val userSchema: StructType = schemaInput  
  private val failFast = ParseModes.isFailFastMode(parseMode)
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  private val permissive = ParseModes.isPermissiveMode(parseMode)

  override val schema: StructType = inferSchema()

  override def buildScan(): RDD[Row] = {
    val simpleDateFormatter = dateFormatter
    val schemaFields = schema.fields
    // val rowArray = new Array[Any](schemaFields.length)
    val baserdd = baseRDD(sqlContext, endpoint)
    print("STart per row conversion")
    val rdd = baserdd.map(r=>Row.fromSeq(Seq(r.toString)))
    print("ENd per row conversion")
    rdd
  }



  private def inferSchema(): StructType = {
    if (this.userSchema != null) {
      userSchema
    } else {
      StructType(Array(StructField("value",StringType,true)))
    }
  }

  

  
}
