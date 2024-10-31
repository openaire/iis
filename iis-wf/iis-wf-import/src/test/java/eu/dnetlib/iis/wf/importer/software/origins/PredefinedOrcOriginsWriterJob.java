package eu.dnetlib.iis.wf.importer.software.origins;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.SparkSessionFactory;

/**
 * Simple class responsible for writing a predefined set of origins encoded as id and url pairs in the ORC format.
 * 
 * @author mhorst
 */
public class PredefinedOrcOriginsWriterJob {

    public static void main(String[] args) throws IOException {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        final String outputPath = params.outputPath;
        
        try (SparkSession sparkSession = SparkSessionFactory.withConfAndKryo(new SparkConf())) {
            JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext()); 
            HdfsUtils.remove(sc.hadoopConfiguration(), outputPath);
            
            List<SourceOrigin> originList = Arrays.asList(
                    new SourceOrigin("1", "http://example.com/1"),
                    new SourceOrigin("2", "http://example.com/2"),
                    new SourceOrigin("3", "http://example.com/3")
            );

            // Create a Dataset from the list of Origin objects
            Dataset<SourceOrigin> originDataset = sparkSession.createDataset(originList, Encoders.bean(SourceOrigin.class));

            // Convert Dataset<Origin> to Dataset<Row> to write as ORC
            Dataset<Row> originDF = originDataset.toDF();

            // Write the dataset in ORC format
            originDF.write().orc(outputPath);
        }
    }
    
    @Parameters(separators = "=")
    public static class JobParameters {

        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
    }
}
