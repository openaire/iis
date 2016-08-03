package eu.dnetlib.iis.wf.citationmatching.direct.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.schemas.ReportParam;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Reporter of citation matching direct job counters.<br/>
 * It calculates citation matching direct counters and saves them
 * as {@link ReportParam} datastore.
 * 
 * @author madryk
 */
public class CitationMatchingDirectCounterReporter {

    private static final String MATCHED_CITATIONS_COUNTER = "export.matchedCitations.direct.total";
    
    private static final String DOCS_WITH_MATCHED_CITATIONS_COUNTER = "export.matchedCitations.direct.docsWithAtLeastOneMatch";
    
    
    private SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Calculates citation matching counters using citations rdd
     * and saves them under outputReportPath.
     */
    public void report(JavaSparkContext sparkContext, JavaRDD<Citation> citations, String outputReportPath) {
        
        ReportParam matchedCitationsCounter = generateMatchedCitationsCounter(citations);
        ReportParam docsWithMatchedCitationsCounter = generateDocsWithCitationsCounter(citations);
        
        JavaRDD<ReportParam> report = sparkContext.parallelize(Lists.newArrayList(matchedCitationsCounter, docsWithMatchedCitationsCounter));
        
        avroSaver.saveJavaRDD(report, ReportParam.SCHEMA$, outputReportPath);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
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
}
