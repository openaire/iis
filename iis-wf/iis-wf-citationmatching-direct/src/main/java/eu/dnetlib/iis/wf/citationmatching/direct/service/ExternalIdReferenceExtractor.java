package eu.dnetlib.iis.wf.citationmatching.direct.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import eu.dnetlib.iis.citationmatching.direct.schemas.Citation;
import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.direct.schemas.ReferenceMetadata;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

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

        return documentsMetadata
                .flatMapToPair(metadata -> {
                    List<Tuple2<String, Citation>> externalIdReferences = Lists.newArrayList();

                    for (ReferenceMetadata referenceMetadata : metadata.getReferences()) {
                        if (referenceMetadata.getExternalIds() == null || !referenceMetadata.getExternalIds().containsKey(idType)) {
                            continue;
                        }

                        String externalId = referenceMetadata.getExternalIds().get(idType).toString();
                        Citation partialCitation = new Citation(metadata.getId(), referenceMetadata.getPosition(), null);
                        externalIdReferences.add(new Tuple2<>(externalId, partialCitation));
                    }

                    return externalIdReferences.iterator();
                });
    }
    
}
