package eu.dnetlib.iis.wf.citationmatching.direct;

import java.io.IOException;
import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Optional;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.wf.citationmatching.direct.service.CitationMatchingDirectCounterReporter;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;


/**
 * Propagates citation matches to unmatched references with the same text.
 * @author mhorst
 *
 */
public class CitationMatchingDirectByTextJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    private static CitationMatchingDirectCounterReporter citationMatchingDirectReporter = new CitationMatchingDirectCounterReporter(
            "processing.citationMatching.directbytext.doc", "processing.citationMatching.directbytext.citDocReference");
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        CitationMatchingDirectByTextJobParameters params = new CitationMatchingDirectByTextJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);
            
            JavaRDD<Citation> allCitations = avroLoader.loadJavaRDD(sc, params.inputCitationsAvroPath, Citation.class);
            JavaRDD<Citation> matchedCitations = avroLoader.loadJavaRDD(sc, params.inputMatchedCitationsAvroPath, Citation.class);
            
            JavaPairRDD<String, Citation> allCitationsByKey = allCitations.mapToPair(cit -> new Tuple2<>(buildCitationKey(cit), cit));
            JavaPairRDD<String, Citation> matchedCitationsByKey = matchedCitations.mapToPair(cit -> new Tuple2<>(buildCitationKey(cit), cit));

            System.out.println("allCitations: " + allCitations.count());
            System.out.println("matchedCitations: " + matchedCitations.count());
            System.out.println("allCitationsByKey: " + allCitationsByKey.count());
            System.out.println("matchedCitationsByKey: " + matchedCitationsByKey.count());
            
            JavaPairRDD<String, Tuple2<Citation,Optional<Citation>>> joinedCitations = allCitationsByKey.leftOuterJoin(matchedCitationsByKey);
            
            JavaRDD<Citation> unmatchedFilteredCitations = joinedCitations
                    .filter(x -> (!x._2._2.isPresent() && isReferenceTextValid(x._2._1.getEntry().getRawText())))
                    .map(x -> x._2._1);
            
            JavaPairRDD<String, Citation> unmatchedCitationsByText = unmatchedFilteredCitations
                    .mapToPair(cit -> new Tuple2<>(normalizeReferenceText(cit.getEntry().getRawText()), cit));
            
            //need to retrive matched citations from joined citations because matchedCitations do not convey text
            JavaPairRDD<String, Citation> matchedCitationsByText = joinedCitations
                    .filter(x -> (x._2._2.isPresent() && isReferenceTextValid(x._2._1.getEntry().getRawText())))
                    .mapToPair(x -> new Tuple2<>(normalizeReferenceText(x._2._1.getEntry().getRawText()), x._2._2.get()));
            
            // distinct removes exact duplicates but we still need to deal with different destinationDocumentId for same sourceDocumentId and position pairs
            // we could remove distinct, group triples, count number of different destinationDocumentId occurences and pick the most common one
            JavaRDD<Citation> outputCitations = unmatchedCitationsByText.join(matchedCitationsByText)
                    .map(x -> propagateMatch(x._2._1, x._2._2));
            
            JavaRDD<Citation> distinctOutputCitations = outputCitations.distinct();
            
            distinctOutputCitations.cache();
            citationMatchingDirectReporter.report(sc, distinctOutputCitations, params.outputReportPath);
            
            avroSaver.saveJavaRDD(distinctOutputCitations, Citation.SCHEMA$, params.outputAvroPath);
        }
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private static String buildCitationKey(Citation cit) {
        StringBuffer strBuff = new StringBuffer(cit.getSourceDocumentId());
        strBuff.append('$');
        strBuff.append(cit.getEntry().getPosition());
        return strBuff.toString();
    }
    
    private static boolean isReferenceTextValid(CharSequence referenceText) {
        // TODO what can we check more? number of ',' '.' ';' occurences?
        return StringUtils.isNotBlank(referenceText) && referenceText.length() > 50;
    }
    
    private static String normalizeReferenceText(CharSequence referenceText) {
        // TODO what can we do more when normalyzing?
        return referenceText.toString().trim();
    }
    
    private static Citation propagateMatch(Citation unmatchedCitation, Citation matchedCitation) {
        return Citation.newBuilder().setSourceDocumentId(unmatchedCitation.getSourceDocumentId())
                .setEntry(CitationEntry.newBuilder()
                        .setPosition(unmatchedCitation.getEntry().getPosition())
                        .setDestinationDocumentId(matchedCitation.getEntry().getDestinationDocumentId())
                        .setConfidenceLevel(matchedCitation.getEntry().getConfidenceLevel())
                        .setExternalDestinationDocumentIds(Collections.emptyMap()).build())
                .build();
    }
    
    @Parameters(separators = "=")
    private static class CitationMatchingDirectByTextJobParameters {
        
        @Parameter(names = "-inputCitationsAvroPath", required = true)
        private String inputCitationsAvroPath;
        
        @Parameter(names = "-inputMatchedCitationsAvroPath", required = true)
        private String inputMatchedCitationsAvroPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
