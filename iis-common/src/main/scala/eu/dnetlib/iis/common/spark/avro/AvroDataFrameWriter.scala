package eu.dnetlib.iis.common.spark.avro

import org.apache.avro.Schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.avro.SchemaConverters

/**
 * Support for writing dataframes of avro types.
 *
 * @param df DataFrame of avro type.
 */
class AvroDataFrameWriter(df: DataFrame) extends Serializable {

  /**
   * Writes a dataframe as avro datastore using avro schema generated from sql schema.
   *
   * @param path Path to the data store.
   * @return
   */
  def write(path: String): Unit = {
    write(path, SchemaConverters.toAvroType(df.schema))
  }

  /**
   * Writes a dataframe as avro datastore using avro schema.
   *
   * @param path       Path to the data store.
   * @param avroSchema Avro schema of the records.
   */
  def write(path: String, avroSchema: Schema): Unit = {
    df
      .write
      .format("avro")
      .option("avroSchema", avroSchema.toString)
      .option("compression", "uncompressed")
      .save(path)
  }
}
