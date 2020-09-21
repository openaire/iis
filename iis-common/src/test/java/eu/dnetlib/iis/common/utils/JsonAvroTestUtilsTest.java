package eu.dnetlib.iis.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import org.junit.Test;

import eu.dnetlib.iis.importer.schemas.DocumentToProject;

/**
 * @author mhorst
 *
 */
public class JsonAvroTestUtilsTest {

    private final String jsonInputFile = ClassPathResourceProvider
            .getResourcePath("eu/dnetlib/iis/common/utils/data/input.json");
    private final String jsonInput2File = ClassPathResourceProvider
            .getResourcePath("eu/dnetlib/iis/common/utils/data/input2.json");

    // ---------------------------------- TESTS -------------------------------------

    @Test
    public void testReadJson() throws Exception {
        // execute
        List<DocumentToProject> results = JsonAvroTestUtils.readJsonDataStore(jsonInputFile, DocumentToProject.class);

        // assert
        assertNotNull(results);
        assertEquals(2, results.size());
        for (int i = 0; i < results.size(); i++) {
            assertEquals("docId-" + (i + 1), results.get(i).getDocumentId().toString());
            assertEquals("projId-" + (i + 1), results.get(i).getProjectId().toString());
        }
    }

    @Test
    public void testReadMultipleJsons() throws Exception {
        // execute
        List<DocumentToProject> results = JsonAvroTestUtils.readMultipleJsonDataStores(
                Arrays.asList(new String[] { jsonInputFile, jsonInput2File }), DocumentToProject.class);

        // assert
        assertNotNull(results);
        assertEquals(4, results.size());
        for (int i = 0; i < results.size(); i++) {
            assertEquals("docId-" + (i + 1), results.get(i).getDocumentId().toString());
            assertEquals("projId-" + (i + 1), results.get(i).getProjectId().toString());
        }
    }

}
