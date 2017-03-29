package eu.dnetlib.iis.common.spark.pipe;

import java.io.File;
import java.io.IOException;

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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.utils.AvroUtils;


/**
 * Spark job that works exacly like hadoop streaming job.<br/>
 * <br/>
 * Job parameters:<br/>
 * -inputAvroPath - directory that contains avro input files<br/>
 * -inputAvroSchemaClass - avro schema class of input<br/>
 * -outputAvroPath - directory for output avro files<br/>
 * -outputAvroSchemaClass - avro schema class of produced output<br/>
 * -mapperScript - path to map script<br/>
 * -mapperScriptArgs - arguments for map script<br/>
 * -reducerScript - path to reduce script<br/>
 * -reducerScriptArgs - arguments for reduce script<br/>
 * 
 * @author madryk
 *
 */
public final class SparkPipeMapReduce {
    
    
    //------------------------ CONSTRUCTORS -------------------
    
    private SparkPipeMapReduce() {}
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        
        
        SparkPipeMapReduceParameters params = parseParameters(args);
        
        SparkConf conf = new SparkConf();
        
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        
        Class<? extends GenericRecord> outputAvroClass = Class.forName(params.outputAvroSchemaClass).asSubclass(GenericRecord.class);
        
        Schema inputSchema = AvroUtils.toSchema(params.inputAvroSchemaClass);
        Schema outputSchema = AvroUtils.toSchema(params.outputAvroSchemaClass);
        Job job = Job.getInstance();
        AvroJob.setInputKeySchema(job, inputSchema);
        AvroJob.setOutputKeySchema(job, outputSchema);
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            sc.addFile(params.mapperScript);
            sc.addFile(params.reducerScript);
            
            String mapperScriptName = new File(params.mapperScript).getName();
            String reducerScriptName = new File(params.reducerScript).getName();
            
            SparkPipeExecutor pipeExecutor = new SparkPipeExecutor();
            
            
            @SuppressWarnings("unchecked")
            JavaPairRDD<AvroKey<GenericRecord>, NullWritable> inputRecords = (JavaPairRDD<AvroKey<GenericRecord>, NullWritable>)sc.newAPIHadoopFile(params.inputAvroPath, AvroKeyInputFormat.class, GenericRecord.class, NullWritable.class, job.getConfiguration());
            
            
            
            JavaPairRDD<String, String> mappedRecords = 
                    pipeExecutor.doMap(inputRecords, mapperScriptName, params.mapperScriptArgs);
            
            
            JavaPairRDD<AvroKey<GenericRecord>, NullWritable> reducedRecords = 
                    pipeExecutor.doReduce(mappedRecords, reducerScriptName, params.reducerScriptArgs, outputAvroClass);
            
            
            
            reducedRecords.saveAsNewAPIHadoopFile(params.outputAvroPath, AvroKey.class, NullWritable.class, AvroKeyOutputFormat.class, job.getConfiguration());
        }
    }


    //------------------------ PRIVATE --------------------------

	private static SparkPipeMapReduceParameters parseParameters(String[] args) {
        SparkPipeMapReduceParameters params = new SparkPipeMapReduceParameters();
        
        JCommander jcommander = new JCommander(params);
        
        jcommander.parse(args);
        
        return params;
    }


    @Parameters(separators = "=")
    private static class SparkPipeMapReduceParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-inputAvroSchemaClass", required = true)
        private String inputAvroSchemaClass;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names = "-outputAvroSchemaClass", required = true)
        private String outputAvroSchemaClass;
        
        @Parameter(names = "-mapperScript", required = true)
        private String mapperScript;
        
        @Parameter(names = "-mapperScriptArgs", required = false)
        private String mapperScriptArgs;
        
        @Parameter(names = "-reducerScript", required = true)
        private String reducerScript;
        
        @Parameter(names = "-reducerScriptArgs", required = false)
        private String reducerScriptArgs;
        
    }
    
}
