package eu.dnetlib.iis.wf.transformers.common.citations.from.referencemetadata;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.PublicationType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CitationsFromReferenceMetadataTransformerJobTest {

    @Test
    void givenReferenceBasicMetadata_whenTransformed_thenBasicMetadataIsPropagated() {
        ReferenceBasicMetadata basicMetadata = ReferenceBasicMetadata.newBuilder()
                .setTitle("Citation title")
                .build();
        ReferenceMetadata reference = ReferenceMetadata.newBuilder()
                .setBasicMetadata(basicMetadata)
                .setPosition(1)
                .setText("raw citation")
                .build();
        ExtractedDocumentMetadataMergedWithOriginal document =
                ExtractedDocumentMetadataMergedWithOriginal.newBuilder()
                        .setId("source-document")
                        .setPublicationType(PublicationType.newBuilder()
                                .setArticle(true)
                                .setDataset(false)
                                .build())
                        .setReferences(Collections.singletonList(reference))
                        .build();

        List<Citation> citations = CitationsFromReferenceMetadataTransformerJob.transformDocument(document);

        assertEquals(1, citations.size());
        assertEquals(basicMetadata, citations.get(0).getEntry().getBasicMetadata());
    }
}
