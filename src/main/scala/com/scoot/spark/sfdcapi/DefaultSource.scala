package com.scoot.spark.sfdcapi

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import com.scoot.spark.sfdcapi.util.SfdcReader

/**
 * Provides access to SFDC data from pure SQL statements (i.e. for users of the
 * JDBC server).
 */
class DefaultSource
  extends RelationProvider
  with SchemaRelationProvider with DataSourceRegister {

  private def checkAPIEndpoint(parameters: Map[String, String]): String = {
    parameters.getOrElse("apiendpoint", sys.error("'apiendpoint' must be specified for SFDC data."))
  }

  override def shortName(): String = "sfdcapi"
  /**
   * Creates a new relation for data store in CSV given parameters.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  /**
   * Creates a new relation for data store in CSV given parameters and user supported schema.
   * Parameters have to include 'path' and optionally 'delimiter', 'quote', and 'header'
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): SfdcApiRelation = {
    val apiEndPoint = checkAPIEndpoint(parameters)

    val parseMode = parameters.getOrElse("mode", "PERMISSIVE")

    val inferSchema = parameters.getOrElse("inferSchema", "false")

    val inferSchemaFlag = if (inferSchema == "false") {
      false
    } else if (inferSchema == "true") {
      true
    } else {
      throw new Exception("Infer schema flag can be true or false")
    }

    val dateFormat = parameters.getOrElse("dateFormat", null)

    
    SfdcApiRelation(
      new SfdcReader().reader,
      apiEndPoint,
      parseMode,
      schema,
      inferSchemaFlag,
      dateFormat)(sqlContext)
  }

  // override def createRelation(
  //     sqlContext: SQLContext,
  //     mode: SaveMode,
  //     parameters: Map[String, String],
  //     data: DataFrame): BaseRelation = {
  //   val path = checkPath(parameters)
  //   val filesystemPath = new Path(path)
  //   val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
  //   val doSave = if (fs.exists(filesystemPath)) {
  //     mode match {
  //       case SaveMode.Append =>
  //         sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName}")
  //       case SaveMode.Overwrite =>
  //         fs.delete(filesystemPath, true)
  //         true
  //       case SaveMode.ErrorIfExists =>
  //         sys.error(s"path $path already exists.")
  //       case SaveMode.Ignore => false
  //     }
  //   } else {
  //     true
  //   }
  //   if (doSave) {
  //     // Only save data when the save mode is not ignore.
  //     val codecClass = CompressionCodecs.getCodecClass(parameters.getOrElse("codec", null))
  //     data.saveAsCsvFile(path, parameters, codecClass)
  //   }

  //   createRelation(sqlContext, parameters, data.schema)
  // }
}
