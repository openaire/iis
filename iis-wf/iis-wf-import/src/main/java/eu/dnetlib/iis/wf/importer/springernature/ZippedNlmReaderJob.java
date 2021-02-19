package eu.dnetlib.iis.wf.importer.springernature;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark job responsible for reading {@link DocumentText} records from zip packages including NLM XMLs provided by Springer Nature.
 *
 * @author mhorst
 */
public class ZippedNlmReaderJob {

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    private static final String COUNTER_READ_TOTAL = "import.zipped.nlm.read.total";
    
    private static final String COUNTER_READ_SIZE_EXCEEDED = "import.zipped.nlm.read.sizeExceeded";
    
    private static final String COUNTER_READ_ARTICLE = "import.zipped.nlm.read.article";
    
    private static final String COUNTER_READ_BOOK = "import.zipped.nlm.read.chapter";
    
    private static final String COUNTER_READ_OTHER = "import.zipped.nlm.read.other";
    
    private static final String FILENAME_INDICATOR_JOURNAL = "JOU=";
    
    private static final String FILENAME_INDICATOR_BOOK = "BOK=";
    
    private static final String OUTPUT_SUBDIR_ARTICLE = "article";
    
    private static final String OUTPUT_SUBDIR_BOOK = "book";
    
    private static final String OUTPUT_SUBDIR_OTHER = "other";
    
    
    public static final Logger log = Logger.getLogger(ZippedNlmReaderJob.class);


    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws Exception {

        ZippedNlmReaderJobParameters params = new ZippedNlmReaderJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        int sizeLimit = Integer.valueOf(params.sizeLimit);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputRootPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            JavaPairRDD<Text, BytesWritable> rawInput = sc.newAPIHadoopFile(params.inputPath, ZipFileInputFormat.class, Text.class, BytesWritable.class, sc.hadoopConfiguration());
            rawInput.cache();
            long totalRawCount = rawInput.count();
            
            JavaRDD<DocumentText> results = rawInput
                    .filter(x -> x._2.getLength() < sizeLimit)
                    .map(x -> DocumentText.newBuilder().setId(x._1.toString()).setText(new String(x._2.getBytes())).build());
            results.cache();
            long resultsCount = results.count();
            
            //cannot check with startsWith because books sometimes ar tested within BSE
            //TODO replace it with regexp to make it more flexible
            JavaRDD<DocumentText> article = results.filter(x -> x.getId().toString().contains(FILENAME_INDICATOR_JOURNAL));
            article.cache();
            JavaRDD<DocumentText> book = results.filter(x -> x.getId().toString().contains(FILENAME_INDICATOR_BOOK));
            book.cache();
            JavaRDD<DocumentText> other = results.subtract(article).subtract(book);
            other.cache();
            
            avroSaver.saveJavaRDD(
                    generateReportEntries(sc, totalRawCount, resultsCount, article.count(), book.count(), other.count()),
                    ReportEntry.SCHEMA$, params.outputReportPath);
            avroSaver.saveJavaRDD(article, DocumentText.SCHEMA$, params.outputRootPath + '/' + OUTPUT_SUBDIR_ARTICLE);
            avroSaver.saveJavaRDD(book, DocumentText.SCHEMA$, params.outputRootPath + '/' + OUTPUT_SUBDIR_BOOK);
            avroSaver.saveJavaRDD(other, DocumentText.SCHEMA$, params.outputRootPath + '/' + OUTPUT_SUBDIR_OTHER);
        }
    }

    //------------------------ PRIVATE --------------------------

    private static JavaRDD<ReportEntry> generateReportEntries(JavaSparkContext sparkContext, 
            long totalRawCount, long acceptedCount, 
            long articleCount, long bookCount, long otherCount) {

        return sparkContext.parallelize(
                Lists.newArrayList(ReportEntryFactory.createCounterReportEntry(COUNTER_READ_TOTAL, acceptedCount),
                        ReportEntryFactory.createCounterReportEntry(COUNTER_READ_SIZE_EXCEEDED, totalRawCount-acceptedCount),
                        ReportEntryFactory.createCounterReportEntry(COUNTER_READ_ARTICLE, articleCount),
                        ReportEntryFactory.createCounterReportEntry(COUNTER_READ_BOOK, bookCount),
                        ReportEntryFactory.createCounterReportEntry(COUNTER_READ_OTHER, otherCount)));
    }

    @Parameters(separators = "=")
    private static class ZippedNlmReaderJobParameters {
        @Parameter(names = "-inputPath", required = true)
        private String inputPath;
        
        @Parameter(names = "-sizeLimit", required = true)
        private String sizeLimit;

        @Parameter(names = "-outputRootPath", required = true)
        private String outputRootPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
