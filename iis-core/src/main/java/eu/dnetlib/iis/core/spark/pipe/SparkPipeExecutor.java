package eu.dnetlib.iis.core.spark.pipe;

import java.io.Serializable;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import eu.dnetlib.iis.core.common.AvroGsonFactory;
import scala.Tuple2;


/**
 * Executor of mapreduce scripts using spark pipes.
 * It imitates hadoop streaming behavior. 
 * 
 * @author madryk
 *
 */
public class SparkPipeExecutor implements Serializable {

	private static final long serialVersionUID = 1L;


	//------------------------ LOGIC --------------------------
	
	/**
	 * Imitates map part of hadoop streaming job
	 */
	public JavaPairRDD<String, String> doMap(JavaPairRDD<AvroKey<GenericRecord>, NullWritable> inputRecords, String scriptName, String args) {

		JavaRDD<String> mappedRecords = inputRecords.keys().pipe(SparkFiles.get(scriptName) + " " + args);

		JavaPairRDD<String, String> outputRecords = mappedRecords
				.mapToPair(line -> {
					String[] splittedPair = line.split("\t");
					return new Tuple2<String, String>(splittedPair[0], (splittedPair.length == 1) ? null : splittedPair[1]);
				});

		return outputRecords;
	}

	/**
	 * Imitates reduce part of hadoop streaming job
	 */
	public JavaPairRDD<AvroKey<GenericRecord>, NullWritable> doReduce(JavaPairRDD<String, String> inputRecords, String scriptName, String args, Class<? extends GenericRecord> outputClass) {

		JavaRDD<String> reducedRecords = inputRecords.sortByKey()
				.map(record -> record._1 + ((record._2 == null) ? "" : ("\t" + record._2)))
				.pipe(SparkFiles.get(scriptName) + " " + args);

		JavaPairRDD<AvroKey<GenericRecord>, NullWritable> outputRecords = reducedRecords
				.map(recordString -> AvroGsonFactory.create().fromJson(recordString, outputClass))
				.mapToPair(record -> new Tuple2<AvroKey<GenericRecord>, NullWritable>(new AvroKey<>(record), NullWritable.get()));

		return outputRecords;
	}

}
