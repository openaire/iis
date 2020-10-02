package eu.dnetlib.iis.wf.transformers.importer.documentmetadata.idextractor;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author mhorst
 *
 */
@IntegrationTest
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testIdExtraction() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/transformers/importer/documentmetadata/idextractor/sampledataproducer");
    }

}
