package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.spark.SparkSessionFactory;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameReader;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameWriter;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlPreMatching;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithMeta;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.SoftwareHeritageOrigin;
import eu.dnetlib.iis.wf.referenceextraction.ReferenceExtractionIOUtils;


/**
 * Job responsible for supplementing software mining output with matchable
 * SoftwareHeritage URLs. Accepts {@link DocumentToSoftwareUrlPreMatching}
 * software mining records at input, supplements them with matchable SH URLs
 * coming from {@link SoftwareHeritageOrigin} records datastore and produces
 * {@link DocumentToSoftwareUrlWithMeta} records.
 * 
 * @author mhorst
 */
public class SoftwareHeritageUrlsMatchingJob {
    
    private static final Logger logger = LoggerFactory.getLogger(SoftwareHeritageUrlsMatchingJob.class);

    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws Exception {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (SparkSession spark = SparkSessionFactory.withConfAndKryo(new SparkConf())) {

            ReferenceExtractionIOUtils.clearOutput(spark, params.outputDocumentToSoftware);

            AvroDataFrameReader avroDataFrameReader = new AvroDataFrameReader(spark);
            
            Dataset<Row> inputDocumentToSoftware = avroDataFrameReader.read(params.inputDocumentToSoftware,
                    DocumentToSoftwareUrlPreMatching.SCHEMA$);
            Dataset<Row> inputSoftwareHeritageOrigin = avroDataFrameReader.read(params.inputSoftwareHeritageOrigin,
                    SoftwareHeritageOrigin.SCHEMA$);
            
            // Perform the left join on the cleanmatch (case-insensitive match)
            Dataset<Row> joinedDF = inputDocumentToSoftware.join(inputSoftwareHeritageOrigin,
                    lower(inputDocumentToSoftware.col("cleanmatch")).equalTo(lower(inputSoftwareHeritageOrigin.col("url"))),
                    "left_outer");
            
            // Add the new column "SHUrl" by concatenating the base URL and the link column
            Dataset<Row> result = joinedDF.withColumn(
                    "SHUrl",
                    concat(lit("https://archive.softwareheritage.org/browse/origin/"), col("url"))
                );
            
            // Select the required columns
            result = result.select(inputDocumentToSoftware.col("documentId"),
                    inputDocumentToSoftware.col("softwareUrl"),
                    inputDocumentToSoftware.col("repositoryName"),
                    inputDocumentToSoftware.col("softwareTitle"), 
                    inputDocumentToSoftware.col("softwareDescription"),
                    inputDocumentToSoftware.col("softwarePageUrl"), 
                    result.col("SHUrl"),
                    inputDocumentToSoftware.col("confidenceLevel"));
            
            storeInOutput(result, params.outputDocumentToSoftware);
        }
    }
    
    //------------------------ PRIVATE --------------------------
    
    private static void storeInOutput(Dataset<Row> resultsToOutputDF, String outputDocumentToSoftware) {
        logger.info("Storing output data in path {}.", outputDocumentToSoftware);
        new AvroDataFrameWriter(resultsToOutputDF).write(outputDocumentToSoftware, DocumentToSoftwareUrlWithMeta.SCHEMA$);
    }
    
    @Parameters(separators = "=")
    public static class JobParameters {

        @Parameter(names = "-inputDocumentToSoftware", required = true)
        private String inputDocumentToSoftware;

        @Parameter(names = "-inputSoftwareHeritageOrigin", required = true)
        private String inputSoftwareHeritageOrigin;

        @Parameter(names = "-outputDocumentToSoftware", required = true)
        private String outputDocumentToSoftware;
    }
}
