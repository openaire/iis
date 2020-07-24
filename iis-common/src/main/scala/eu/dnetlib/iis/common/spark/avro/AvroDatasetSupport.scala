package eu.dnetlib.iis.common.spark.avro

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.apache.spark.sql._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType

/**
 * Spark avro datasource supporting functions for datasets.
 *
 * @param spark SparkSession instance.
 */
class AvroDatasetSupport(val spark: SparkSession) extends Serializable {

  /**
   * Reads data as a dataset from an avro data store using kryo encoder.
   *
   * NOTE: due to inability to use bean based encoder for avro types this method uses kryo encoder;
   * for this reason this method creates objects by mapping rows to jsons and jsons to instances of objects.
   *
   * @param path       Path to the data store.
   * @param avroSchema Avro schema of the records.
   * @param clazz      Class of objects in the dataset.
   * @tparam T Type of objects in the dataset.
   * @return Dataset with data read from given path.
   */
  def read[T <: SpecificRecordBase](path: String, avroSchema: Schema, clazz: Class[T]): Dataset[T] = {
    implicit val encoder: Encoder[T] = Encoders.kryo(clazz)
    val mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    spark.read
      .format("avro")
      .option("avroSchema", avroSchema.toString)
      .load(path)
      .toJSON
      .map(json => mapper.readValue(json, clazz))
  }

  /**
   * Writes a dataset as an avro data store using an avro schema.
   *
   * @param ds         Dataset to be saved as avro data store.
   * @param path       Path to the data store.
   * @param avroSchema Avro schema of the records.
   * @tparam T Type of objects in the dataset.
   */
  def write[T <: SpecificRecordBase](ds: Dataset[T], path: String, avroSchema: Schema): Unit = {
    toDF(ds, avroSchema)
      .write
      .format("avro")
      .option("avroSchema", avroSchema.toString)
      .save(path)
  }

  /**
   * Creates a dataframe from given dataset.
   *
   * @param ds         Dataset to be converted to a dataframe.
   * @param avroSchema Avro schema of the records.
   * @tparam T Type of objects in the dataset.
   * @return DataFrame of objects corresponding to records in the given dataset.
   */
  def toDF[T <: SpecificRecordBase](ds: Dataset[T], avroSchema: Schema): DataFrame = {
    val avroSchemaStr = avroSchema.toString
    val rowSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
    val encoder = RowEncoder(rowSchema).resolveAndBind()

    object SerializationSupport extends Serializable {
      @transient private lazy val deserializer = new AvroDeserializer(new Schema.Parser().parse(avroSchemaStr), rowSchema)
      private val rows = ds.rdd.map(record => encoder.fromRow(deserializer.deserialize(record).asInstanceOf[InternalRow]))

      def doToDF(): DataFrame = {
        spark.createDataFrame(rows, rowSchema)
      }
    }

    SerializationSupport.doToDF()
  }
}
