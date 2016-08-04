package eu.dnetlib.iis.wf.citationmatching.direct.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Reporter of citation matching direct job counters.<br/>
 * It calculates citation matching direct counters and saves them
 * as {@link ReportEntry} datastore.
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
        
        ReportEntry matchedCitationsCounter = generateMatchedCitationsCounter(citations);
        ReportEntry docsWithMatchedCitationsCounter = generateDocsWithCitationsCounter(citations);
        
        JavaRDD<ReportEntry> report = sparkContext.parallelize(Lists.newArrayList(matchedCitationsCounter, docsWithMatchedCitationsCounter));
        
        avroSaver.saveJavaRDD(report, ReportEntry.SCHEMA$, outputReportPath);
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private ReportEntry generateMatchedCitationsCounter(JavaRDD<Citation> matchedCitations) {
        
        long citationsCount = matchedCitations.count();
        
        return ReportEntryFactory.createCounterReportEntry(MATCHED_CITATIONS_COUNTER, citationsCount);
        
    }
    
    private ReportEntry generateDocsWithCitationsCounter(JavaRDD<Citation> matchedCitations) {
        
        long docsWithCitationCount = matchedCitations
                .map(x -> x.getSourceDocumentId().toString())
                .distinct().count();
        
        return ReportEntryFactory.createCounterReportEntry(DOCS_WITH_MATCHED_CITATIONS_COUNTER, docsWithCitationCount);
    }
}
