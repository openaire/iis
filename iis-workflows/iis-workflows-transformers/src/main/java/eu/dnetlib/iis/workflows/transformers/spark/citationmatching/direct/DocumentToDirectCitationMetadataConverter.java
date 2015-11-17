package eu.dnetlib.iis.workflows.transformers.spark.citationmatching.direct;

import java.io.Serializable;
import java.util.List;

import org.apache.commons.collections.MapUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.citationmatching.direct.schemas.DocumentMetadata;
import eu.dnetlib.iis.citationmatching.direct.schemas.ReferenceMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;

/**
 * Converter of {@link ExtractedDocumentMetadataMergedWithOriginal} object
 * to {@link DocumentMetadata} object
 * 
 * @author madryk
 *
 */
public class DocumentToDirectCitationMetadataConverter implements Serializable {

    private static final long serialVersionUID = 1L;


    //------------------------ LOGIC --------------------------

    /**
     * Converts {@link ExtractedDocumentMetadataMergedWithOriginal} to {@link DocumentMetadata}
     */
    public DocumentMetadata convert(ExtractedDocumentMetadataMergedWithOriginal docMetadata) {
        Preconditions.checkNotNull(docMetadata);

        List<ReferenceMetadata> citationReferencesMetadata = convertReferences(docMetadata.getReferences());

        return new DocumentMetadata(docMetadata.getId(), docMetadata.getExternalIdentifiers(), docMetadata.getPublicationTypeName(),
                citationReferencesMetadata);
    }


    //------------------------ PRIVATE --------------------------

    private List<ReferenceMetadata> convertReferences(List<eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata> referencesMetadata) {
        List<ReferenceMetadata> citationReferencesMetadata = Lists.newArrayList();

        if (referencesMetadata != null) {
            for (eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata docReferenceMetadata : referencesMetadata) {

                ReferenceMetadata citationReferenceMetadata = convertReference(docReferenceMetadata);

                if (citationReferenceMetadata != null) {
                    citationReferencesMetadata.add(citationReferenceMetadata);
                }
            }
        }

        return citationReferencesMetadata;
    }

    private ReferenceMetadata convertReference(eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata refMetadata) {

        if (MapUtils.isEmpty(refMetadata.getBasicMetadata().getExternalIds())) {
            return null;
        }

        return new eu.dnetlib.iis.citationmatching.direct.schemas.ReferenceMetadata(
                refMetadata.getPosition(),
                refMetadata.getBasicMetadata().getExternalIds());
    }
}
