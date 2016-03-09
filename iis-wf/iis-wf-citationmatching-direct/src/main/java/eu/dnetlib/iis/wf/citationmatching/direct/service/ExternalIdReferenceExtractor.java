package eu.dnetlib.iis.wf.citationmatching.direct.service;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.direct.schemas.ReferenceMetadata;
import scala.Tuple2;

/**
 * Extractor of document references
 * 
 * @author madryk
 *
 */
public class ExternalIdReferenceExtractor implements Serializable {

    private static final long serialVersionUID = 1L;


    //------------------------ LOGIC --------------------------
    
    /**
     * Extracts document references based on external id with {@literal idType} type.
     * 
     * @param documentsMetadata
     * @param idType - type of external identifier (e.g. doi, pmid)
     * @return pair rdd where keys are external ids of {@literal idType} type
     *      and values are partially filled {@link Citation} objects 
     *      (with null {@link Citation#getDestinationDocumentId()})
     */
    public JavaPairRDD<String, Citation> extractExternalIdReferences(JavaRDD<DocumentMetadata> documentsMetadata, String idType) {
        Preconditions.checkNotNull(documentsMetadata);
        Preconditions.checkNotNull(idType);
        
        JavaPairRDD<String, Citation> externalIdReferencesRdd = documentsMetadata
                .flatMapToPair(metadata -> {
                    List<Tuple2<String, Citation>> externalIdReferences = Lists.newArrayList();

                    for (ReferenceMetadata referenceMetadata : metadata.getReferences()) {

                        if (referenceMetadata.getExternalIds() == null || !referenceMetadata.getExternalIds().containsKey(idType)) {
                            continue;
                        }

                        String externalId = referenceMetadata.getExternalIds().get(idType).toString();
                        Citation partialCitation = new Citation(metadata.getId(), referenceMetadata.getPosition(), null);

                        externalIdReferences.add(new Tuple2<String, Citation>(externalId, partialCitation));
                    }

                    return externalIdReferences;
                });

        return externalIdReferencesRdd;
    }
    
}
