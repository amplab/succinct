package edu.berkeley.cs.succinct.sql

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, SchemaRelationProvider, RelationProvider}

class DefaultSource
  extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for Succinct data."))
  }
  
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SuccinctRelation(checkPath(parameters))(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    SuccinctRelation(checkPath(parameters), schema)(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext, 
      mode: SaveMode, 
      parameters: Map[String, String], 
      data: DataFrame): BaseRelation = {
    val path = parameters("path")
    val filesystemPath = new Path(path)
    val fs = filesystemPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)
    val doSave = if (fs.exists(filesystemPath)) {
      mode match {
        case SaveMode.Append =>
          sys.error(s"Append mode is not supported by ${this.getClass.getName}")
        case SaveMode.Overwrite =>
          fs.delete(filesystemPath, true)
          true
        case SaveMode.ErrorIfExists =>
          sys.error(s"path $path already exists.")
        case SaveMode.Ignore =>
          false
      }
    } else {
      true
    }
    
    if(doSave) {
      data.saveAsSuccinctFiles(path)
    }
    createRelation(sqlContext, parameters, data.schema)
  }
}
