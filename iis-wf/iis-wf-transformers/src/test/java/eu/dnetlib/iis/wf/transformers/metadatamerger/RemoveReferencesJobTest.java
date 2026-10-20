package eu.dnetlib.iis.wf.transformers.metadatamerger;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;

import org.junit.jupiter.api.Test;

import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.PublicationType;

class RemoveReferencesJobTest {

    @Test
    void clearsReferencesAndPreservesOtherMetadata() {
        ExtractedDocumentMetadataMergedWithOriginal input =
                ExtractedDocumentMetadataMergedWithOriginal.newBuilder()
                        .setId("document-1")
                        .setTitle("A title")
                        .setPublicationType(PublicationType.newBuilder().build())
                        .setReferences(Collections.emptyList())
                        .build();

        ExtractedDocumentMetadataMergedWithOriginal output = RemoveReferencesJob.removeReferences(input);

        assertNull(output.getReferences());
        assertEquals(input.getId(), output.getId());
        assertEquals(input.getTitle(), output.getTitle());
        assertEquals(input.getPublicationType(), output.getPublicationType());
    }
}
