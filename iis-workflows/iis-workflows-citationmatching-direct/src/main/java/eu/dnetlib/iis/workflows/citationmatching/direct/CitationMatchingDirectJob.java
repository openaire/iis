package eu.dnetlib.iis.workflows.citationmatching.direct;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.common.spark.avro.SparkAvroLoader;
import eu.dnetlib.iis.common.spark.avro.SparkAvroSaver;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.workflows.citationmatching.direct.converters.DirectCitationToCitationConverter;
import eu.dnetlib.iis.workflows.citationmatching.direct.converters.DocumentToDirectCitationMetadataConverter;
import eu.dnetlib.iis.workflows.citationmatching.direct.model.IdWithPosition;
import eu.dnetlib.iis.workflows.citationmatching.direct.service.ExternalIdCitationMatcher;


public class CitationMatchingDirectJob {
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
        CitationMatchingDirectJobParameters params = new CitationMatchingDirectJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "eu.dnetlib.iis.core.spark.AvroCompatibleKryoRegistrator");
        
        
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> documentsMetadata = SparkAvroLoader.loadJavaRDD(sc, params.inputAvroPath, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            
            DocumentToDirectCitationMetadataConverter documentMetadataConverter = new DocumentToDirectCitationMetadataConverter();
            JavaRDD<DocumentMetadata> simplifiedDocumentsMetadata = documentsMetadata.map(metadata -> documentMetadataConverter.convert(metadata));
//            simplifiedDocumentsMetadata = simplifiedDocumentsMetadata.cache(); // FIXME: https://github.com/openaire/iis/issues/128
            
            JavaRDD<Citation> directDoiCitations = matchDoiCitations(simplifiedDocumentsMetadata);
            
            JavaRDD<Citation> directPmidCitations = matchPmidCitations(simplifiedDocumentsMetadata);
            
            JavaRDD<Citation> directCitations = mergeCitations(directDoiCitations, directPmidCitations);
            
            
            
            DirectCitationToCitationConverter directCitationToCitationConverter = new DirectCitationToCitationConverter();
            JavaRDD<eu.dnetlib.iis.common.citations.schemas.Citation> citations = 
                    directCitations.map(directCitation -> directCitationToCitationConverter.convert(directCitation));
            
            
            SparkAvroSaver.saveJavaRDD(citations, eu.dnetlib.iis.common.citations.schemas.Citation.SCHEMA$, params.outputAvroPath);
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    
    private static JavaRDD<Citation> matchDoiCitations(JavaRDD<DocumentMetadata> directCitationMetadata) {
        ExternalIdCitationMatcher externalIdCitationMatcher = new ExternalIdCitationMatcher();
        
        return externalIdCitationMatcher.matchCitations(directCitationMetadata, "doi", x -> x.iterator().next());
    }
    
    
    private static JavaRDD<Citation> matchPmidCitations(JavaRDD<DocumentMetadata> directCitationMetadata) {
        ExternalIdCitationMatcher externalIdCitationMatcher = new ExternalIdCitationMatcher();
        
        return externalIdCitationMatcher.matchCitations(directCitationMetadata, "pmid", x -> {
            Iterator<DocumentMetadata> it = x.iterator();
            
            DocumentMetadata current = null;
            while(it.hasNext()) {
                DocumentMetadata docMeta = it.next();
                if (StringUtils.equals(docMeta.getPublicationTypeName(), "research-article")) {
                    return docMeta;
                }
                
                current = docMeta;
            }
            return current;
        });
    }
    
    
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
        
        
    }
}
