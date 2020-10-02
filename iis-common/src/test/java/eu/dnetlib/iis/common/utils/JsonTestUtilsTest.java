package eu.dnetlib.iis.common.utils;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author mhorst
 *
 */
public class JsonTestUtilsTest {

    private final String jsonInputFile = ClassPathResourceProvider
            .getResourcePath("eu/dnetlib/iis/common/utils/data/input.json");
    private final String jsonInput2File = ClassPathResourceProvider
            .getResourcePath("eu/dnetlib/iis/common/utils/data/input2.json");

    private static final class DocumentToProject {

        private String documentId;

        private String projectId;

        public String getDocumentId() {
            return documentId;
        }

        public String getProjectId() {
            return projectId;
        }
    }

    // ---------------------------------- TESTS -------------------------------------

    @Test
    public void testReadJson() throws Exception {
        // execute
        List<DocumentToProject> results = JsonTestUtils.readJson(jsonInputFile, DocumentToProject.class);

        // assert
        assertNotNull(results);
        assertEquals(2, results.size());
        for (int i = 0; i < results.size(); i++) {
            assertEquals("docId-" + (i + 1), results.get(i).getDocumentId());
            assertEquals("projId-" + (i + 1), results.get(i).getProjectId());
        }
    }

    @Test
    public void testReadMultipleJsons() throws Exception {
        // execute
        List<DocumentToProject> results = JsonTestUtils.readMultipleJsons(
                Arrays.asList(new String[] { jsonInputFile, jsonInput2File }), DocumentToProject.class);

        // assert
        assertNotNull(results);
        assertEquals(4, results.size());
        for (int i = 0; i < results.size(); i++) {
            assertEquals("docId-" + (i + 1), results.get(i).getDocumentId());
            assertEquals("projId-" + (i + 1), results.get(i).getProjectId());
        }
    }

}
