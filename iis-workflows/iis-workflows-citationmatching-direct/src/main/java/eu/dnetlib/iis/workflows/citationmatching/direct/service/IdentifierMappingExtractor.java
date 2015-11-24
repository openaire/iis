package eu.dnetlib.iis.workflows.citationmatching.direct.service;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;


/**
 * Extractor of external id to internal id mapping
 * 
 * @author madryk
 *
 */
public class IdentifierMappingExtractor implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Extracts from document metadata mapping from external id to internal id
     * 
     * @param documentsMetadata
     * @param idType - type of external identifier (e.g. doi, pmid)
     * @param pickSingle - function used in case there will be more than one document with the same external identifier.
     *      Function should pick one of those documents and return it as a result.
     * @return pair rdd where keys are external ids of {@literal idType} type
     *      and values are documents ids
     */
    public JavaPairRDD<String, String> extractIdMapping(JavaRDD<DocumentMetadata> documentsMetadata, String idType, Function<Iterable<DocumentMetadata>, DocumentMetadata> pickSingle) {
        
        JavaPairRDD<String, String> doiToId = documentsMetadata
                .filter(docMetadata -> docMetadata.getExternalIdentifiers() != null && docMetadata.getExternalIdentifiers().containsKey(idType))
                .keyBy(docMetadata -> docMetadata.getExternalIdentifiers().get(idType).toString())
                .groupByKey()
                .mapValues(pickSingle)
                .mapValues(docMetadata -> docMetadata.getId().toString());
        
        return doiToId;
        
    }
}
