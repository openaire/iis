package eu.dnetlib.iis.wf.importer.infospace.converter;

import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import org.junit.jupiter.api.BeforeEach;

/**
 * Tests for {@link DocumentToProjectRelationConverter}
 */
public class DocumentToProjectRelationConverterTest extends OafRelToAvroConverterTestBase<DocumentToProject> {

    @BeforeEach
    public void setUp() {
        converter = new DocumentToProjectRelationConverter();
        getSourceId = DocumentToProject::getDocumentId;
        getTargetId = DocumentToProject::getProjectId;
    }

    // ------------------------ TESTS --------------------------
    // All tests are in the base class
}
