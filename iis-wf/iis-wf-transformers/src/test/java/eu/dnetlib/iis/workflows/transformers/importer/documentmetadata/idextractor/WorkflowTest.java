package eu.dnetlib.iis.workflows.transformers.importer.documentmetadata.idextractor;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

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
