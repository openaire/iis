package eu.dnetlib.iis.core.examples.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.spark.avro.AvroSaver;
import eu.dnetlib.iis.core.common.AvroUtils;

/**
 * A spark generic avro cloner which uses sparkSql to read and write avro data. Parameters: 
   
        -inputAvroHdfsFilePath - hdfs path to file(s) with records that are supposed to be cloned
        -inputAvroClass - fully qualified name of the class generated from avro schema
        -outputAvroHdfsFilePath - hdfs path the cloned avro records will saved to
        -numberOfCopies - number of copies of each avro record
   
   @author Łukasz Dumiszewski
   
 */

public class SparkSqlAvroCloner {
    
    
    
    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        
        SparkClonerParameters params = new SparkClonerParameters();
        
        JCommander jcommander = new JCommander(params);
        
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
       
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        
        Schema schema = AvroUtils.toSchema(params.avroSchemaClass);
        Job job = Job.getInstance();
        AvroJob.setInputKeySchema(job, schema);
        AvroJob.setOutputKeySchema(job, schema);
        
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            SQLContext sqlContext = new SQLContext(sc);
            
            
            DataFrame inputDf = sqlContext.load(params.inputAvroPath, "com.databricks.spark.avro");

           
            
            JavaRDD<Row> rows = inputDf.javaRDD();
           
            
            
            int numberOfCopies = params.numberOfCopies;
            
            rows = rows.flatMap(row -> {List<Row> clonedRows = new ArrayList<>(numberOfCopies);
                                                  for (int i=0; i<numberOfCopies; i++) {clonedRows.add(row);}
                                                  return clonedRows;});

            
            DataFrame outputDf = sqlContext.createDataFrame(rows, inputDf.schema());
            
            AvroSaver.save(outputDf, schema, params.outputAvroPath);
            
        }
        
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    @Parameters(separators = "=")
    private static class SparkClonerParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-avroSchemaClass", required = true, description = "fully qualified name of the class generated from avro schema")
        private String avroSchemaClass;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names= "-numberOfCopies", required = true)
        private int numberOfCopies;
        
        
    }
}
