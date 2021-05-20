package eu.dnetlib.iis.common.spark.avro

import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Support for reading avro datastores as dataframes.
 *
 * @param spark SparkSession instance.
 */
class AvroDataFrameReader(val spark: SparkSession) extends Serializable {

  /**
   * Reads avro datastore as Spark dataframe using SQL schema.
   *
   * @param path   Path to the datastore.
   * @param schema SQL schema of the records.
   * @return DataFrame with data read from given path.
   */
  def read(path: String, schema: StructType): DataFrame = {
    read(path, SchemaConverters.toAvroType(schema))
  }

  /**
   * Reads avro datastore as Spark dataframe using avro schema.
   *
   * @param path       Path to the data store.
   * @param avroSchema Avro schema of the records.
   * @return DataFrame with data read from given path.
   */
  def read(path: String, avroSchema: Schema): DataFrame = {
    val in = spark.read
      .format("avro")
      .option("avroSchema", avroSchema.toString)
      .load(path)
    spark.createDataFrame(in.rdd, SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType])
  }
}
