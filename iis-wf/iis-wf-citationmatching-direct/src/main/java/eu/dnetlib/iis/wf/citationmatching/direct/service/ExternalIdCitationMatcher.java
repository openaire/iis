package eu.dnetlib.iis.wf.citationmatching.direct.service;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import scala.Tuple2;

/**
 * Matcher of citations from {@link DocumentMetadata} rdd based on 
 * identity of external ids with same type.
 * 
 * @author madryk
 *
 */
public class ExternalIdCitationMatcher implements Serializable {

    private static final long serialVersionUID = 1L;


    private final IdentifierMappingExtractor idMappingExtractor = new IdentifierMappingExtractor();
    
    private final ExternalIdReferenceExtractor referencePicker = new ExternalIdReferenceExtractor();
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * 
     * Matches citations from {@link DocumentMetadata} rdd based
     * on identity of external ids with type {@literal idType}.<br/>
     * That is:
     * <br/>
     * Citation from first document (document1) to second document (document2) will be found
     * if one of document1 references have external id which is equal to
     * external id of document2 (considering that both external ids are of {@literal idType}).
     * <br/>
     * <br/>
     * For example, function will find a citation for two below documents:
     * <pre>
     * {id: "document1", references: [ {externalIds: {"pmid": "123456"}} ], ... }
     * {id: "document2", externalIdentifiers: {"pmid": "123456"}, ... }
     * </pre>
     * Assuming that {@literal idType} is <code>pmid</code>
     * 
     * @param documents - 
     * @param idType - type of external identifier (e.g. doi, pmid)
     * @param pickSingle - function used in case there will be more than one document with the same external identifier.
     *      Function should pick one of those documents and return it as a result.
     * @return rdd of matched citations
     */
    public JavaRDD<Citation> matchCitations(JavaRDD<DocumentMetadata> documents, String idType, Function<Iterable<DocumentMetadata>, DocumentMetadata> pickSingle) {
        Preconditions.checkNotNull(documents);
        Preconditions.checkNotNull(idType);
        Preconditions.checkNotNull(pickSingle);
        
        
        JavaPairRDD<String, String> externalIdToIdMapping = idMappingExtractor.extractIdMapping(documents, idType, pickSingle);
        
        
        JavaPairRDD<String, Citation> externalIdReferences = referencePicker.extractExternalIdReferences(documents, idType);
        
        
        JavaRDD<Citation> externalIdCitation = externalIdReferences.join(externalIdToIdMapping)
                .map(x -> {
                    Citation partialCitation = x._2._1;
                    String destinationDocumentId = x._2._2;
                    
                    return Citation.newBuilder(partialCitation)
                            .setDestinationDocumentId(destinationDocumentId)
                            .build();
                });
        
        
        return externalIdCitation;
    }
    
    /**
     * Matches citations from {@link DocumentMetadata} rdd based on document external ids with type {@literal mainIdType} 
     * mappable to {@literal referenceIdType} from bibliographic reference using identifier mappings defined in {@literal referenceIdMappings}.<br/>
     * 
     * That is:
     * <br/>
     * Citation from first document (document1) to second document (document2) will be found
     * if one of document1 references have external id which is mappable to
     * external id of document2 (considering that there is a mapping defined in {@literal referenceIdMappings} between the two identifier values).
     * <br/>
     * <br/>
     * For example, function will find a citation for two below documents:
     * <pre>
     * {id: "document1", references: [ {externalIds: {"pmid": "123456"}} ], ... }
     * {id: "document2", externalIdentifiers: {"pmc": "PMC34567"}, ... }
     * </pre>
     * Assuming that {@literal mainIdType} is <code>pmc</code>, {@literal referenceIdType} is <code>pmid</code> and there is appriopriate
     * mapping entry <"123456","PMC34567"> defined among {@literal referenceIdMappings}.
     * 
     * @param documents input publications
     * @param referenceIdMappings identifiers mappings between reference identifier and publication identifier
     * @param mainIdType external identifier type of publication (e.g. pmc)
     * @param referenceIdType external identifier type of bibliographic reference (e.g. pmid)
     * @param pickSingle function used in case there will be more than one document with the same external identifier.
     *      Function should pick one of those documents and return it as a result.
     * @return rdd of matched citations
     */
    public JavaRDD<Citation> matchCitations(JavaRDD<DocumentMetadata> documents, JavaPairRDD<String, String> referenceIdMappings, 
            String mainIdType, String referenceIdType, Function<Iterable<DocumentMetadata>, DocumentMetadata> pickSingle) {
        Preconditions.checkNotNull(documents);
        Preconditions.checkNotNull(referenceIdMappings);
        Preconditions.checkNotNull(mainIdType);
        Preconditions.checkNotNull(referenceIdType);
        Preconditions.checkNotNull(pickSingle);
        
        JavaPairRDD<String, String> externalIdToIdMapping = idMappingExtractor.extractIdMapping(documents, mainIdType, pickSingle);
        
        JavaPairRDD<String, Citation> externalIdReferences = referencePicker.extractExternalIdReferences(documents, referenceIdType)
                .join(referenceIdMappings).mapToPair(x -> new Tuple2<String, Citation>(x._2._2,x._2._1));
        
        JavaRDD<Citation> externalIdCitation = externalIdReferences.join(externalIdToIdMapping)
                .map(x -> {
                    Citation partialCitation = x._2._1;
                    String destinationDocumentId = x._2._2;
                    
                    return Citation.newBuilder(partialCitation)
                            .setDestinationDocumentId(destinationDocumentId)
                            .build();
                });
        
        return externalIdCitation;
    }
}
