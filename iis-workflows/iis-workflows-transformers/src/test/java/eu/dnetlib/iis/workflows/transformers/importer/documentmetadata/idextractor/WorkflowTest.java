package eu.dnetlib.iis.workflows.transformers.importer.documentmetadata.idextractor;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * 
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

    @Test
    public void testIdExtraction() throws Exception {
        runWorkflow("eu/dnetlib/iis/workflows/transformers/importer/documentmetadata/idextractor/sampledataproducer/oozie_app");
    }

}
