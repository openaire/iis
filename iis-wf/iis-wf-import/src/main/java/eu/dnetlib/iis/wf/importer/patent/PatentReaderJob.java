package eu.dnetlib.iis.wf.importer.patent;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark job responsible for reading {@link Patent} records from JSON file.
 * @author mhorst
 *
 */
public class PatentReaderJob {
    
private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    public static final Logger log = Logger.getLogger(PatentReaderJob.class);
    
    private static final String COUNTER_READ_TOTAL = "import.patent.read.total";
    
    private static final String FIELD_APPLN_ID = "appln_id";
    
    private static final String FIELD_APPLN_AUTH = "appln_auth";
    
    private static final String FIELD_APPLN_NR = "appln_nr";
    
    private static final String FIELD_APPLN_NR_EPODOC = "appln_nr_epodoc";
    
    
    
    //------------------------ LOGIC --------------------------
    
    
    public static void main(String[] args) throws Exception {
        
        PatentReaderJobParameters params = new PatentReaderJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            Configuration hadoopConf = sc.hadoopConfiguration();
            
            HdfsUtils.remove(hadoopConf, params.outputPath);
            HdfsUtils.remove(hadoopConf, params.outputReportPath);
            
            SQLContext sqlContext = new SQLContext(sc);
            
            //TODO consider explicit schema definition using StructType
            
            JavaRDD<Patent> results = sqlContext.read().json(params.inputJSONLocation).toJavaRDD().map(e -> buildEntry(e));
            
            storeInOutput(results, generateReportEntries(sc, results), params.outputPath, params.outputReportPath);
        }
    }
    
    //------------------------ PRIVATE --------------------------
    

    private static Patent buildEntry(Row row) {
        Patent.Builder patentBuilder = Patent.newBuilder();
        patentBuilder.setApplnId(row.getAs(FIELD_APPLN_ID));
        patentBuilder.setApplnAuth(row.getAs(FIELD_APPLN_AUTH));
        patentBuilder.setApplnNr(row.getAs(FIELD_APPLN_NR));
        patentBuilder.setApplnNrEpodoc(row.getAs(FIELD_APPLN_NR_EPODOC));
        return patentBuilder.build();
    }
    
    private static JavaRDD<ReportEntry> generateReportEntries(JavaSparkContext sparkContext, 
            JavaRDD<Patent> entries) {
        
        Preconditions.checkNotNull(sparkContext, "sparkContext has not been set");
        
        ReportEntry fromCacheEntitiesCounter = ReportEntryFactory.createCounterReportEntry(COUNTER_READ_TOTAL, entries.count());
        
        return sparkContext.parallelize(Lists.newArrayList(fromCacheEntitiesCounter));
    }

    
    private static void storeInOutput(JavaRDD<Patent> results, JavaRDD<ReportEntry> reports, 
            String resultOutputPath, String reportOutputPath) {
        avroSaver.saveJavaRDD(results, Patent.SCHEMA$, resultOutputPath);
        avroSaver.saveJavaRDD(reports, ReportEntry.SCHEMA$, reportOutputPath);
    }
    
    @Parameters(separators = "=")
    private static class PatentReaderJobParameters {
        
        @Parameter(names = "-inputJSONLocation", required = true)
        private String inputJSONLocation;
        
        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;

    }
}
