package eu.dnetlib.iis.wf.citationmatching.direct.service;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.google.common.base.Preconditions;

import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;

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
}
