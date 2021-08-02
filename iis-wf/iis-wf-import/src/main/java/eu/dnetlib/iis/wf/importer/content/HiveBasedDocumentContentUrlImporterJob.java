package eu.dnetlib.iis.wf.importer.content;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.spark.SparkConfHelper;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;


/**
 * {@link DocumentContentUrl} importer reading data from hive table being a part of pdf aggregation subsystem. 
 * 
 * @author mhorst
 *
 */
public class HiveBasedDocumentContentUrlImporterJob {

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    public static void main(String[] args) throws Exception {

        HiveBasedDocumentContentUrlImporterJobParameters params = new HiveBasedDocumentContentUrlImporterJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        SparkConf conf = SparkConfHelper.withKryo(new SparkConf());
        // conf.registerKryoClasses(OafModelUtils.provideOafClasses());
        conf.set("hive.metastore.uris", params.hiveMetastoreUris);
        
        SparkSession sparkSession = SparkSession.builder()
                //.config(SparkConfHelper.withKryo(conf))
                .config(conf)
                .enableHiveSupport()
                .getOrCreate();        
        
        // sparkSession.sql("show databases").show();
        // sparkSession.sql("show tables").show();

        // this shows "hive" so the hive is set properly, not in-memory due to the lack of hive libs on classpath - this is fine
        // System.out.println("catalog impl: " + sparkSession.conf().get("spark.sql.catalogImplementation"));
        
        // just to debug, shows proper number for openaire_prod but 0 for pdfaggregation
        sparkSession.sql("select id from openaire_prod_20210716.publication limit 10").show();
        
        // testing on a non empty table
        Dataset<Row> pubsResult = sparkSession.sql("select id, originalid from openaire_prod_20210716.publication");
        pubsResult.cache();
        Dataset<Row> idResults = pubsResult.select(pubsResult.col("id"));
        System.out.println("number of imported publication results: " + idResults.count());
        idResults.write().csv(params.outputPath+"_csv_test");
        // end of testing
        
        Dataset<Row> result = sparkSession.sql("select id, actual_url, mimetype, size, hash from " + params.inputTableName);
        // number of imported records is 0;
        System.out.println("number of imported results: " + result.count());
        
        JavaRDD<DocumentContentUrl> documentContentUrl = buildOutputRecord(result, sparkSession);
        
        avroSaver.saveJavaRDD(documentContentUrl, DocumentContentUrl.SCHEMA$, params.outputPath);
    }
    
    private static JavaRDD<DocumentContentUrl> buildOutputRecord(Dataset<Row> source, SparkSession spark) {
        Dataset<Row> resultDs = source.select(
                concat(lit("50|"), col("id")).as("id"),
                col("actual_url").as("url"),
                col("mimetype").as("mimeType"),
                col("size").cast("long").divide(1024).as("contentSizeKB"),
                col("hash").as("contentChecksum")
                );
        return new AvroDataFrameSupport(spark).toDS(resultDs, DocumentContentUrl.class).toJavaRDD();
    }
    
    @Parameters(separators = "=")
    private static class HiveBasedDocumentContentUrlImporterJobParameters {
        
        @Parameter(names = "-inputTableName", required = true)
        private String inputTableName;
        
        @Parameter(names = "-hiveMetastoreUris", required = true)
        private String hiveMetastoreUris;
        
        @Parameter(names = "-outputPath", required = true)
        private String outputPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
    
}
