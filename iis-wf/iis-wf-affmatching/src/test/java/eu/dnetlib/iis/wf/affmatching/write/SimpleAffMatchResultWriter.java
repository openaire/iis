package eu.dnetlib.iis.wf.affmatching.write;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import eu.dnetlib.iis.wf.affmatching.model.SimpleAffMatchResult;

/**
 * Implementation of {@link AffMatchResultWriter} used for quality testing purposes.<br/>
 * It writes affiliation matches in simplified form.
 * 
 * @author madryk
 */
public class SimpleAffMatchResultWriter implements AffMatchResultWriter {

    private static final long serialVersionUID = 1L;

    /**
     * Writes the given rdd of {@link AffMatchResult}s under the given path
     * as json records {@link SimpleAffMatchResult}
     */
    @Override
    public void write(JavaSparkContext sc, JavaRDD<AffMatchResult> matchedAffOrgs, String outputPath, String outputReportPath, int numberOfEmittedFiles) {
        
        
        JavaRDD<String> simpleMatchedAffOrgs = matchedAffOrgs
                .map(match -> new SimpleAffMatchResult(match.getAffiliation().getDocumentId(), match.getAffiliation().getPosition(), match.getOrganization().getId()))
                .map(simpleMatch -> new Gson().toJson(simpleMatch))
                .coalesce(1);
        
        simpleMatchedAffOrgs.saveAsTextFile(outputPath);
    }

}
