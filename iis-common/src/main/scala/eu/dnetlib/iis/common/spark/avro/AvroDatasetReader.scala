package eu.dnetlib.iis.common.spark.avro

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Support for reading avro datastores as datasets.
 *
 * @param spark SparkSession instance.
 */
class AvroDatasetReader(val spark: SparkSession) extends Serializable {

  /**
   * Reads avro datastore as Spark dataset using avro schema and kryo encoder.
   *
   * NOTE: due to inability to use bean-based encoder for avro types this method uses kryo encoder;
   * for this reason this method creates objects by mapping rows to jsons and jsons to instances of objects.
   *
   * @param path       Path to the data store.
   * @param avroSchema Avro schema of the records.
   * @param clazz      Class of objects in the dataset.
   * @tparam T Type of objects in the dataset.
   * @return Dataset with data read from given path.
   */
  def read[T <: SpecificRecordBase](path: String, avroSchema: Schema, clazz: Class[T]): Dataset[T] = {
    new AvroDataFrameSupport(spark).toDS(new AvroDataFrameReader(spark).read(path, avroSchema), clazz)
  }
}
