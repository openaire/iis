package eu.dnetlib.iis.wf.citationmatching;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.common.schemas.ReportParam;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Reporter of citation matching job counters.<br/>
 * It calculates citation matching counters and saves them
 * as {@link ReportParam} datastore.
 * 
 * @author madryk
 */
public class CitationMatchingCounterReporter {

    private static final String MATCHED_CITATIONS_COUNTER = "export.matchedCitations.fuzzy.total";
    
    private static final String DOCS_WITH_MATCHED_CITATIONS_COUNTER = "export.matchedCitations.fuzzy.docsWithAtLeastOneMatch";
    
    
    private SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    private String reportPath;
    
    private JavaSparkContext sparkContext;
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Calculates citation matching counters using matchedCitations rdd
     * and saves them under {@link #setReportPath(String)}
     */
    public void report(JavaRDD<Citation> matchedCitations) {
        
        checkState();
        
        ReportParam matchedCitationsCounter = generateMatchedCitationsCounter(matchedCitations);
        ReportParam docsWithMatchedCitationsCounter = generateDocsWithCitationsCounter(matchedCitations);
        
        
        JavaRDD<ReportParam> report = sparkContext.parallelize(Lists.newArrayList(matchedCitationsCounter, docsWithMatchedCitationsCounter));
        
        avroSaver.saveJavaRDD(report, ReportParam.SCHEMA$, reportPath);
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void checkState() {
        
        Preconditions.checkNotNull(reportPath, "reportPath has not been set");
        Preconditions.checkNotNull(sparkContext, "sparkContext has not been set");
    }
    
    private ReportParam generateMatchedCitationsCounter(JavaRDD<Citation> matchedCitations) {
        
        long citationsCount = matchedCitations.count();
        
        return new ReportParam(MATCHED_CITATIONS_COUNTER, String.valueOf(citationsCount));
        
    }
    
    private ReportParam generateDocsWithCitationsCounter(JavaRDD<Citation> matchedCitations) {
        
        long docsWithCitationCount = matchedCitations
                .map(x -> x.getSourceDocumentId().toString())
                .distinct().count();
        
        return new ReportParam(DOCS_WITH_MATCHED_CITATIONS_COUNTER, String.valueOf(docsWithCitationCount));
    }


    //------------------------ SETTERS --------------------------
    
    public void setReportPath(String reportPath) {
        this.reportPath = reportPath;
    }

    public void setSparkContext(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }
}
