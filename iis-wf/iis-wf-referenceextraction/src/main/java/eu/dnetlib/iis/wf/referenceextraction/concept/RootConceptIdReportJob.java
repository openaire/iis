package eu.dnetlib.iis.wf.referenceextraction.concept;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.schemas.ReportEntryType;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentToConceptId;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

import java.io.IOException;

/**
 * Generates {@link DocumentToConceptId} relation counters grouped by root concept identifiers.
 * 
 * @author mhorst
 *
 */
public class RootConceptIdReportJob {
    
    private static final String CONCEPT_SEPARATOR = "::";
    
    private static final String ROOT_CONCEPT_ID_TOKEN = "#{rootConceptId}";
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        RootConceptIdReportJobParameters params = new RootConceptIdReportJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);
            
            JavaRDD<DocumentToConceptId> documentToConcept = avroLoader
                    .loadJavaRDD(sc, params.inputDocumentToConceptAvroPath, DocumentToConceptId.class);

            JavaPairRDD<CharSequence, Integer> rootConceptIdToOne = documentToConcept
                    .mapToPair(x -> new Tuple2<>(extractRootConceptId(x.getConceptId()), 1));
            JavaPairRDD<CharSequence, Integer> reducedRootConceptIdWithCount = rootConceptIdToOne
                    .reduceByKey(Integer::sum);

            JavaRDD<ReportEntry> conceptReport = convertToReportEntries(reducedRootConceptIdWithCount
                    .sortByKey(true), params.reportKeyTemplate);
            
            avroSaver.saveJavaRDD(conceptReport, ReportEntry.SCHEMA$, params.outputReportPath);
        }
        
    }
    
    //------------------------ PRIVATE --------------------------

    /**
     * Extracts root contcept identifier from specific conceptId.
     * Never returns null, "unknown" value is returned when funder detail is not available. 
     */
    private static String extractRootConceptId(CharSequence conceptId) {
        if (StringUtils.isNotBlank(conceptId)) {
            String conceptIdStr = conceptId.toString();
            if (conceptIdStr.contains(CONCEPT_SEPARATOR)) {
                if (!conceptIdStr.startsWith(CONCEPT_SEPARATOR)) {
                    String rootConceptId = StringUtils.splitByWholeSeparator(conceptIdStr, CONCEPT_SEPARATOR)[0];
                    if (!StringUtils.isBlank(rootConceptId)) {
                        return rootConceptId;
                    }    
                }
            } else {
                return conceptIdStr;
            }
        }
        return "unknown";
    }
    
    /**
     * Converts all funder names into report entry keys using template and replacing FUNDER_TOKEN with real funder name.
     */
    private static JavaRDD<ReportEntry> convertToReportEntries(JavaPairRDD<CharSequence, Integer> source,
                                                               String reportKeyTemplate) {
        return source.map(x -> new ReportEntry(
                StringUtils.replace(reportKeyTemplate, ROOT_CONCEPT_ID_TOKEN, x._1.toString().toLowerCase()), 
                ReportEntryType.COUNTER, String.valueOf(x._2)));
    }
    
    @Parameters(separators = "=")
    private static class RootConceptIdReportJobParameters {

        @Parameter(names = "-inputDocumentToConceptAvroPath", required = true)
        private String inputDocumentToConceptAvroPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
        @Parameter(names = "-reportKeyTemplate", required = true)
        private String reportKeyTemplate;

    }
}
