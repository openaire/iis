package eu.dnetlib.iis.common.spark.pipe;

import eu.dnetlib.iis.common.utils.AvroGsonFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;

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
	 * Imitates map part of hadoop streaming job.
	 * It executes provided script for every key in inputRecords rdd.
	 * <br/><br/>
	 * It is assumed that provided script will read records from standard input (one line for one record)
	 * and write mapped record into standard output (also one line for one record).
	 * Mapped record can be a key/value pair. In that case script should return key and value
	 * splitted by tab (\t) character in single line. 
	 */
	public JavaPairRDD<String, String> doMap(JavaPairRDD<AvroKey<GenericRecord>, NullWritable> inputRecords, String scriptName, String args) {
		JavaRDD<String> mappedRecords = inputRecords
				.keys()
				.pipe("python " + SparkFiles.get(scriptName) + " " + args);
		return mappedRecords
				.mapToPair(line -> {
					String[] splittedPair = line.split("\t");
					return new Tuple2<>(splittedPair[0], (splittedPair.length == 1) ? null : splittedPair[1]);
				});
	}

	/**
	 * Imitates reduce part of hadoop streaming job.
	 * <br/><br/>
	 * It is assumed that provided script will read records from standard input (one line for one record)
	 * and group records with the same key into single record (reduce).
	 * Method assures that all input records with the same key will be transfered in adjacent lines.
	 * Reduced records should be written by script into standard output (one line for one record).
	 * Reduced records must be json strings of class provided as argument.
	 */
	public JavaPairRDD<AvroKey<GenericRecord>, NullWritable> doReduce(JavaPairRDD<String, String> inputRecords, String scriptName, String args, Class<? extends GenericRecord> outputClass) {
		JavaRDD<String> reducedRecords = inputRecords
				.sortByKey()
				.map(record -> record._1 + ((record._2 == null) ? "" : ("\t" + record._2)))
				.pipe("python " + SparkFiles.get(scriptName) + " " + args);
		return reducedRecords
				.map(recordString -> AvroGsonFactory.create().fromJson(recordString, outputClass))
				.mapToPair(record -> new Tuple2<>(new AvroKey<>(record), NullWritable.get()));
	}

}
