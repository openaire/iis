package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InferredDocumentProjectConverterTest {

    private InferredDocumentProjectConverter converter = new InferredDocumentProjectConverter();

    private final String docId = "docId";
    private final String projId = "projId";
    private final float confidenceLevel = 0.9f;

    // ------------------------ TESTS --------------------------

    @Test
    public void convert_null() {
        // execute
        assertThrows(NullPointerException.class, () -> converter.convert(null));
    }

    @Test
    public void convert_blank_document_id() {
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                converter.convert(new DocumentToProject(" ", projId, 1f, null)));
    }

    @Test
    public void convert_blank_project_id() {
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                converter.convert(new DocumentToProject(docId, " ", 1f, null)));
    }

    @Test
    public void convert_out_of_right_range_confidence_level() {
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                converter.convert(new DocumentToProject(docId, projId, 2f, null)));
    }

    @Test
    public void convert_out_of_left_range_confidence_level() {
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                converter.convert(new DocumentToProject(docId, projId, -1f, null)));
    }

    @Test
    public void convert_null_document_id() {
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                converter.convert(new DocumentToProject(null, projId, confidenceLevel, null)));
    }

    @Test
    public void convert_null_project_id() {
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                converter.convert(new DocumentToProject(docId, null, confidenceLevel, null)));
    }

    @Test
    public void convert() {
        // execute
        AffMatchDocumentProject result = converter.convert(new DocumentToProject(docId, projId, confidenceLevel, null));
        // assert
        assertEquals(docId, result.getDocumentId());
        assertEquals(projId, result.getProjectId());
        assertEquals(confidenceLevel, result.getConfidenceLevel(), 0);
    }

}
