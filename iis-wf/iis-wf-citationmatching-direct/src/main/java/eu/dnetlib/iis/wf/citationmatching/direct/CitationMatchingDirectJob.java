package eu.dnetlib.iis.wf.citationmatching.direct;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.wf.citationmatching.direct.converter.DirectCitationToCitationConverter;
import eu.dnetlib.iis.wf.citationmatching.direct.converter.DocumentToDirectCitationMetadataConverter;
import eu.dnetlib.iis.wf.citationmatching.direct.model.IdWithPosition;
import eu.dnetlib.iis.wf.citationmatching.direct.service.CitationMatchingDirectCounterReporter;
import eu.dnetlib.iis.wf.citationmatching.direct.service.ExternalIdCitationMatcher;
import eu.dnetlib.iis.wf.citationmatching.direct.service.PickFirstDocumentFunction;
import eu.dnetlib.iis.wf.citationmatching.direct.service.PickResearchArticleDocumentFunction;


public class CitationMatchingDirectJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    private static DocumentToDirectCitationMetadataConverter documentToDirectCitationMetadataConverter = new DocumentToDirectCitationMetadataConverter();
    
    private static ExternalIdCitationMatcher externalIdCitationMatcher = new ExternalIdCitationMatcher();
    
    private static DirectCitationToCitationConverter directCitationToCitationConverter = new DirectCitationToCitationConverter();
    
    private static CitationMatchingDirectCounterReporter citationMatchingDirectReporter = new CitationMatchingDirectCounterReporter();
    
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        CitationMatchingDirectJobParameters params = new CitationMatchingDirectJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> documents = avroLoader.loadJavaRDD(sc, params.inputAvroPath, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            
            JavaRDD<DocumentMetadata> simplifiedDocuments = documents.map(document -> documentToDirectCitationMetadataConverter.convert(document));
            simplifiedDocuments = simplifiedDocuments.cache();
            
            
            JavaRDD<Citation> directDoiCitations = externalIdCitationMatcher.matchCitations(simplifiedDocuments, "doi", new PickFirstDocumentFunction());
            
            JavaRDD<Citation> directPmidCitations = externalIdCitationMatcher.matchCitations(simplifiedDocuments, "pmid", new PickResearchArticleDocumentFunction());
            
            JavaRDD<Citation> directCitations = mergeCitations(directDoiCitations, directPmidCitations);
            
            
            
            JavaRDD<eu.dnetlib.iis.common.citations.schemas.Citation> citations = 
                    directCitations.map(directCitation -> directCitationToCitationConverter.convert(directCitation));
            
            citations.cache();
            citationMatchingDirectReporter.report(sc, citations, params.outputReportPath);
            
            avroSaver.saveJavaRDD(citations, eu.dnetlib.iis.common.citations.schemas.Citation.SCHEMA$, params.outputAvroPath);
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    
    private static JavaRDD<Citation> mergeCitations(JavaRDD<Citation> directDoiCitations, JavaRDD<Citation> directPmidCitations) {
        
        JavaPairRDD<IdWithPosition, Citation> directDoiCitationsWithKey = attachIdWithPositionKey(directDoiCitations);
        JavaPairRDD<IdWithPosition, Citation> directPmidCitationsWithKey = attachIdWithPositionKey(directPmidCitations);
        
        JavaRDD<Citation> directCitations = directDoiCitationsWithKey.fullOuterJoin(directPmidCitationsWithKey)
                .map(x -> x._2._1.isPresent() ? x._2._1.get() : x._2._2.get() );
        
        return directCitations;
    }
    
    private static JavaPairRDD<IdWithPosition, Citation> attachIdWithPositionKey(JavaRDD<Citation> directCitations) {
        JavaPairRDD<IdWithPosition, Citation> directCitationsWithKey = directCitations
                .keyBy(directCitation -> new IdWithPosition(directCitation.getSourceDocumentId().toString(), directCitation.getPosition()));
        
        return directCitationsWithKey;
    }
    
    
    @Parameters(separators = "=")
    private static class CitationMatchingDirectJobParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
