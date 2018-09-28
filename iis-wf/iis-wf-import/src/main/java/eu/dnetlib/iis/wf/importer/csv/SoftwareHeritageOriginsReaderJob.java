package eu.dnetlib.iis.wf.importer.csv;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.SoftwareHeritageOrigin;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;


/**
 * Software Heritage origin and url pairs reader. Reads data from CSV file and produces avro datastore with {@link SoftwareHeritageOrigin} records.
 * 
 * @author mhorst
 *
 */
public class SoftwareHeritageOriginsReaderJob {
    
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    public static final Logger log = Logger.getLogger(SoftwareHeritageOriginsReaderJob.class);
    
    private static final String COUNTER_SH_READ_TOTAL = "processing.referenceExtraction.softwareUrl.softwareheritage.read.total";
    
    
    //------------------------ LOGIC --------------------------
    
    
    public static void main(String[] args) throws Exception {
        
        SoftwareOriginsReaderJobParameters params = new SoftwareOriginsReaderJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            Configuration hadoopConf = sc.hadoopConfiguration();
            
            HdfsUtils.remove(hadoopConf, params.outputPath);
            HdfsUtils.remove(hadoopConf, params.outputReportPath);
            
            JavaRDD<SoftwareHeritageOrigin> results = sc.textFile(params.inputCSVLocation).map(e -> buildEntry(e));
            
            storeInOutput(results, generateReportEntries(sc, results), params.outputPath, params.outputReportPath);
        }
    }
    
    //------------------------ PRIVATE --------------------------
    

    private static SoftwareHeritageOrigin buildEntry(String line) {
        SoftwareHeritageOrigin.Builder shBuilder = SoftwareHeritageOrigin.newBuilder();
        String[] split = StringUtils.split(line, ',');
        shBuilder.setOrigin(split[0]);
        shBuilder.setUrl(split[1]);
        return shBuilder.build();
    }
    
    private static JavaRDD<ReportEntry> generateReportEntries(JavaSparkContext sparkContext, 
            JavaRDD<SoftwareHeritageOrigin> entries) {
        
        Preconditions.checkNotNull(sparkContext, "sparkContext has not been set");
        
        ReportEntry fromCacheEntitiesCounter = ReportEntryFactory.createCounterReportEntry(COUNTER_SH_READ_TOTAL, entries.count());
        
        return sparkContext.parallelize(Lists.newArrayList(fromCacheEntitiesCounter));
    }

    
    private static void storeInOutput(JavaRDD<SoftwareHeritageOrigin> results, JavaRDD<ReportEntry> reports, 
            String resultOutputPath, String reportOutputPath) {
        avroSaver.saveJavaRDD(results, SoftwareHeritageOrigin.SCHEMA$, resultOutputPath);
        avroSaver.saveJavaRDD(reports, ReportEntry.SCHEMA$, reportOutputPath);
    }
    
    @Parameters(separators = "=")
    private static class SoftwareOriginsReaderJobParameters {
        
        @Parameter(names = "-inputCSVLocation", required = true)
        private String inputCSVLocation;
        
        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;

    }
}
