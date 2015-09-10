/*
 * Copyright (c) 2014-2014 ICM UW
 */

package eu.dnetlib.iis.collapsers

import org.apache.avro.generic.GenericData
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapred.AvroValue
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConverters._

/**
 * @author Michal Oniszczuk (m.oniszczuk@icm.edu.pl)
 *         Created: 30.04.2014 09:45
 */
class DefaultCollapserReducer extends Reducer[AvroKey[String], AvroValue[IndexedRecord], AvroKey[IndexedRecord], NullWritable] {
  private var origins: List[String] = null


  override def setup(context: Reducer[AvroKey[String], AvroValue[IndexedRecord], AvroKey[IndexedRecord], NullWritable]#Context) {
    origins = splitByComma(getOrigins(context))
  }

  private def getOrigins(context: Reducer[AvroKey[String], AvroValue[IndexedRecord], AvroKey[IndexedRecord], NullWritable]#Context) =
    getWorkflowParameter(context, "origins")

  private def getWorkflowParameter(context: Reducer[AvroKey[String], AvroValue[IndexedRecord], AvroKey[IndexedRecord], NullWritable]#Context, parameterName: String) =
    context.getConfiguration.get(parameterName)

  private def splitByComma(origins: String) =
    origins.split(',').toList


  override def reduce(key: AvroKey[String], values: java.lang.Iterable[AvroValue[IndexedRecord]], context: Reducer[AvroKey[String], AvroValue[IndexedRecord], AvroKey[IndexedRecord], NullWritable]#Context) {
    val records = mapWithDatumDeepCopy(values.asScala)
    val bestOrigin = selectBestAvailableOrigin(records, origins)
    val bestRecords = records
      .filter(record => getOriginField(record) == bestOrigin)
      .map(getDataField)
    writeRecords(context, bestRecords)
  }

  private def mapWithDatumDeepCopy(values: Iterable[AvroValue[IndexedRecord]]): Iterable[IndexedRecord] =
    values.map(record => getDeepCopy(record.datum()))

  private def selectBestAvailableOrigin(records: Iterable[IndexedRecord], origins: List[String]): String = {
    val availableOrigins = records.map(getOriginField)
    availableOrigins.minBy(origin => origins.indexOf(origin))
  }

  private def getOriginField(record: IndexedRecord): String =
    getField(record, "origin")

  private def getDataField(record: IndexedRecord): IndexedRecord =
    getField(record, "data")

  private def getField[T](record: IndexedRecord, fieldName: String): T = {
    val fieldPosition: Int = record.getSchema.getField(fieldName).pos
    record
      .get(fieldPosition)
      .asInstanceOf[T]
  }

  private def getDeepCopy(value: IndexedRecord): IndexedRecord = {
    GenericData.get.deepCopy(value.getSchema, value)
  }

  private def writeRecords(context: Reducer[AvroKey[String], AvroValue[IndexedRecord], AvroKey[IndexedRecord], NullWritable]#Context, records: Iterable[IndexedRecord]) {
    records.map(record => writeRecord(context, record))
  }

  private def writeRecord(context: Reducer[AvroKey[String], AvroValue[IndexedRecord], AvroKey[IndexedRecord], NullWritable]#Context, record: IndexedRecord) {
    context.write(new AvroKey[IndexedRecord](record), NullWritable.get)
  }
}
