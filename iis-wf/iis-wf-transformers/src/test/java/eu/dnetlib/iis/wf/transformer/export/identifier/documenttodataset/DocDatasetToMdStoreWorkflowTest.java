package eu.dnetlib.iis.wf.transformer.export.identifier.documenttodataset;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;

/**
* @author Łukasz Dumiszewski
*/
@Category(IntegrationTest.class)
public class DocDatasetToMdStoreWorkflowTest extends AbstractOozieWorkflowTestCase {
    
    @Test
    public void testWorkflow() throws Exception {
        OozieWorkflowTestConfiguration wf = new OozieWorkflowTestConfiguration();
        testWorkflow("eu/dnetlib/iis/wf/transformers/export/identifier/documenttodataset/test", wf);
    }
    
}
