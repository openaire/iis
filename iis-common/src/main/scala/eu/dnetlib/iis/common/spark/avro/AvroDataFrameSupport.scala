package eu.dnetlib.iis.common.spark.avro

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.apache.spark.sql._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * Support for dataframes of avro types.
 *
 * @param spark SparkSession instance.
 */
class AvroDataFrameSupport(val spark: SparkSession) extends Serializable {

  /**
   * Creates a dataframe from a given collection.
   *
   * @param data       List with elements for the dataframe.
   * @param avroSchema Avro schema of the elements.
   * @tparam T Type of elements.
   * @return DataFrame containing data from the given list.
   */
//  def createDataFrame[T](data: java.util.List[T], avroSchema: Schema): DataFrame = {
//    createDataFrame(data.asScala, avroSchema)
//  }

  /**
   * Creates a dataframe from a given collection.
   *
   * @param data       Seq with elements for the dataframe.
   * @param avroSchema Avro schema of the elements.
   * @tparam T Type of elements.
   * @return DataFrame containing data from the given seq.
   */
//  def createDataFrame[T](data: Seq[T], avroSchema: Schema): DataFrame = {
//    val rowSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
//    val encoder = RowEncoder.apply(rowSchema).resolveAndBind()
//    val deserializer = new AvroDeserializer(avroSchema, rowSchema)
//    val rows = data.map(record => encoder.fromRow(deserializer.deserialize(record).asInstanceOf[InternalRow]))
//    spark.createDataFrame(spark.sparkContext.parallelize(rows), rowSchema)
//  }

  /**
   * Creates a dataset from given dataframe using kryo encoder.
   *
   * NOTE: due to inability to use bean based encoder for avro types this method uses kryo encoder;
   * for this reason this method creates objects by mapping rows to jsons and jsons to instances of objects.
   *
   * @param df    DataFrame to be converted to a dataset.
   * @param clazz Class of objects in the dataset.
   * @tparam T Type of objects in the dataset.
   * @return Dataset of objects corresponding to records in the given dataframe.
   */
  def toDS[T <: SpecificRecordBase](df: DataFrame, clazz: Class[T]): Dataset[T] = {
    implicit val encoder: Encoder[T] = Encoders.kryo(clazz)
    val mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    df
      .toJSON
      .map(json => mapper.readValue(json, clazz))
  }
}
