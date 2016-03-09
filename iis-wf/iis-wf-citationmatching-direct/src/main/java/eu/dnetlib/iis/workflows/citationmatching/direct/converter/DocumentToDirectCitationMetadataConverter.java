package eu.dnetlib.iis.workflows.citationmatching.direct.converter;

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
    public DocumentMetadata convert(ExtractedDocumentMetadataMergedWithOriginal document) {
        Preconditions.checkNotNull(document);

        List<ReferenceMetadata> citationReferencesMetadata = convertReferences(document.getReferences());

        return new DocumentMetadata(document.getId(), document.getExternalIdentifiers(), document.getPublicationTypeName(),
                citationReferencesMetadata);
    }


    //------------------------ PRIVATE --------------------------

    private List<ReferenceMetadata> convertReferences(List<eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata> documentReferences) {
        List<ReferenceMetadata> citationReferencesMetadata = Lists.newArrayList();

        if (documentReferences != null) {
            for (eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata documentReference : documentReferences) {

                ReferenceMetadata citationReferenceMetadata = convertReference(documentReference);

                if (citationReferenceMetadata != null) {
                    citationReferencesMetadata.add(citationReferenceMetadata);
                }
            }
        }

        return citationReferencesMetadata;
    }

    private ReferenceMetadata convertReference(eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata documentReference) {

        if (MapUtils.isEmpty(documentReference.getBasicMetadata().getExternalIds())) {
            return null;
        }

        return new eu.dnetlib.iis.citationmatching.direct.schemas.ReferenceMetadata(
                documentReference.getPosition(),
                documentReference.getBasicMetadata().getExternalIds());
    }
}
