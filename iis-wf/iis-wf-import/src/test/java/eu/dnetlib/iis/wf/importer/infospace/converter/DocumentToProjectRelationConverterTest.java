package eu.dnetlib.iis.wf.importer.infospace.converter;

import org.junit.Before;

import eu.dnetlib.iis.importer.schemas.DocumentToProject;

/**
 * Tests for {@link DocumentToProjectRelationConverter}
 */
public class DocumentToProjectRelationConverterTest extends OafRelToAvroConverterTestBase<DocumentToProject> {

    @Before
    public void setUp() {
        converter = new DocumentToProjectRelationConverter();
        getSourceId = DocumentToProject::getDocumentId;
        getTargetId = DocumentToProject::getProjectId;
    }

    // ------------------------ TESTS --------------------------
    // All tests are in the base class
}
