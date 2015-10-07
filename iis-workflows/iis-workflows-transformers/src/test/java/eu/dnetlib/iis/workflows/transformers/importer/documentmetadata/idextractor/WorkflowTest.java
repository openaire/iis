package eu.dnetlib.iis.workflows.transformers.importer.documentmetadata.idextractor;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractOozieWorkflowTestCase;

/**
 * 
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testIdExtraction() throws Exception {
    	testWorkflow("eu/dnetlib/iis/workflows/transformers/importer/documentmetadata/idextractor/sampledataproducer");
    }

}
