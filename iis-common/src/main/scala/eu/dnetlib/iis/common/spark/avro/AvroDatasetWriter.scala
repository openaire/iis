package eu.dnetlib.iis.common.spark.avro

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.apache.spark.sql.Dataset

/**
 * Support for writing datasets of avro types.
 *
 * @param ds Dataset of avro type.
 * @tparam T Avro type.
 */
class AvroDatasetWriter[T <: SpecificRecordBase](ds: Dataset[T]) extends Serializable {

  /**
   * Writes a dataset as avro datastore using avro schema.
   *
   * @param path       Path to the data store.
   * @param avroSchema Avro schema of the records.
   */
  def write(path: String, avroSchema: Schema): Unit = {
    new AvroDatasetSupport(ds.sparkSession).toDF(ds, avroSchema)
      .write
      .format("avro")
      .option("avroSchema", avroSchema.toString)
      .save(path)
  }
}
