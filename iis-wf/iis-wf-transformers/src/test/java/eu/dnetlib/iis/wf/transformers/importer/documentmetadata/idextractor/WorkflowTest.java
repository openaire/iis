package eu.dnetlib.iis.wf.transformers.importer.documentmetadata.idextractor;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * 
 * @author mhorst
 *
 */
public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testIdExtraction() throws Exception {
    	testWorkflow("eu/dnetlib/iis/wf/transformers/importer/documentmetadata/idextractor/sampledataproducer");
    }

}
