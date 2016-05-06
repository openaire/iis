package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentProjectConverter;

public class DocumentProjectConverterTest {

    private DocumentProjectConverter converter = new DocumentProjectConverter();

    private final String docId = "docId";
    private final String projId = "projId";
    private final float confidenceLevel = 0.9f;

    // ------------------------ TESTS --------------------------

    @Test(expected = NullPointerException.class)
    public void convert_null() {
        // execute
        converter.convert(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void convert_blank_document_id() {
        // execute
        converter.convert(new DocumentToProject(" ", projId, 1f));
    }

    @Test(expected = IllegalArgumentException.class)
    public void convert_blank_project_id() {
        // execute
        converter.convert(new DocumentToProject(docId, " ", 1f));
    }

    @Test(expected = IllegalArgumentException.class)
    public void convert_out_of_right_range_confidence_level() {
        // execute
        converter.convert(new DocumentToProject(docId, projId, 2f));
    }

    @Test(expected = IllegalArgumentException.class)
    public void convert_out_of_left_range_confidence_level() {
        // execute
        converter.convert(new DocumentToProject(docId, projId, -1f));
    }

    @Test(expected = IllegalArgumentException.class)
    public void convert_null_document_id() {
        // execute
        converter.convert(new DocumentToProject(null, projId, confidenceLevel));
    }

    @Test(expected = IllegalArgumentException.class)
    public void convert_null_project_id() {
        // execute
        converter.convert(new DocumentToProject(docId, null, confidenceLevel));
    }

    @Test(expected = IllegalArgumentException.class)
    public void convert_null_confidence_level() {
        // execute
        converter.convert(new DocumentToProject(docId, projId, null));
    }

    @Test
    public void convert() {
        // execute
        AffMatchDocumentProject result = converter.convert(new DocumentToProject(docId, projId, confidenceLevel));
        // assert
        assertEquals(docId, result.getDocumentId());
        assertEquals(projId, result.getProjectId());
        assertEquals(confidenceLevel, result.getConfidenceLevel(), 0);
    }

}
