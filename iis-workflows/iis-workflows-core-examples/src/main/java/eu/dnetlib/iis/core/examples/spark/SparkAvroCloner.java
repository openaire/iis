package eu.dnetlib.iis.core.examples.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.core.common.AvroUtils;

/**
 * A spark generic avro cloner. Parameters: 
   
        -inputAvroHdfsFilePath - hdfs path to file(s) with records that are supposed to be cloned
        -inputAvroClass - fully qualified name of the class generated from avro schema
        -outputAvroHdfsFilePath - hdfs path the cloned avro records will saved to
        -numberOfCopies - number of copies of each avro record
   
   @author Łukasz Dumiszewski
   
 */

public class SparkAvroCloner {
    
    
    
    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        
        SparkClonerParameters params = new SparkClonerParameters();
        
        JCommander jcommander = new JCommander(params);
        
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
       
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        
        Schema schema = AvroUtils.toSchema(params.inputAvroClass);
        Job job = Job.getInstance();
        AvroJob.setInputKeySchema(job, schema);
        AvroJob.setOutputKeySchema(job, schema);
        
       
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
    
            @SuppressWarnings("unchecked")
            JavaPairRDD<AvroKey<GenericRecord>, NullWritable> inputRecords = (JavaPairRDD<AvroKey<GenericRecord>, NullWritable>)sc.newAPIHadoopFile(params.inputAvroHdfsFilePath, AvroKeyInputFormat.class, GenericRecord.class, NullWritable.class, job.getConfiguration());
        
            
            int numberOfCopies = params.numberOfCopies;
            
            inputRecords = inputRecords.flatMapToPair(record -> {List<Tuple2<AvroKey<GenericRecord>, NullWritable>> records = new ArrayList<>(numberOfCopies);
                                                                 for (int i=0; i<numberOfCopies; i++) {records.add(record);}
                                                                 return records;});
            
            inputRecords.saveAsNewAPIHadoopFile(params.outputAvroHdfsFilePath, AvroKey.class, NullWritable.class, AvroKeyOutputFormat.class, job.getConfiguration());
        
            
        }
        
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    @Parameters(separators = "=")
    private static class SparkClonerParameters {
        
        @Parameter(names = "-inputAvroHdfsFilePath", required = true)
        private String inputAvroHdfsFilePath;
        
        @Parameter(names = "-inputAvroClass", required = true, description = "fully qualified name of the class generated from avro schema")
        private String inputAvroClass;
        
        @Parameter(names = "-outputAvroHdfsFilePath", required = true)
        private String outputAvroHdfsFilePath;
        
        @Parameter(names= "-numberOfCopies", required = true)
        private int numberOfCopies;
        
        
    }
}
