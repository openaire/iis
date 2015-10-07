package eu.dnetlib.iis.workflows.transformers.metadataextraction;

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
    public void testChecksumPreprocessing() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/transformers/metadataextraction/checksum/preprocessing/sampledataproducer");
    }
    
    @Test
    public void testChecksumPostprocessingText() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/transformers/metadataextraction/checksum/postprocessing/text/sampledataproducer");
    }
    
    @Test
    public void testChecksumPostprocessingMeta() throws Exception {
        testWorkflow("eu/dnetlib/iis/workflows/transformers/metadataextraction/checksum/postprocessing/meta/sampledataproducer");
    }
}
