package eu.dnetlib.iis.common.spark.avro

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.apache.spark.sql._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType

/**
 * Support for datasets of avro types.
 *
 * @param spark SparkSession instance.
 */
class AvroDatasetSupport(val spark: SparkSession) extends Serializable {

  /**
   * Creates a dataframe from given dataset of avro type.
   *
   * @param ds         Dataset to be converted to a dataframe.
   * @param avroSchema Avro schema of the records.
   * @tparam T Type of objects in the dataset.
   * @return DataFrame of objects corresponding to records in the given dataset.
   */
//  def toDF[T <: SpecificRecordBase](ds: Dataset[T], avroSchema: Schema): DataFrame = {
//    val avroSchemaStr = avroSchema.toString
//    val rowSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
//    val encoder = RowEncoder(rowSchema).resolveAndBind()
//
//    object SerializationSupport extends Serializable {
//      @transient private lazy val deserializer = new AvroDeserializer(new Schema.Parser().parse(avroSchemaStr), rowSchema)
//      private val rows = ds.rdd.map(record => encoder.fromRow(deserializer.deserialize(record).asInstanceOf[InternalRow]))
//
//      def doToDF(): DataFrame = {
//        spark.createDataFrame(rows, rowSchema)
//      }
//    }
//
//    SerializationSupport.doToDF()
//  }
}
