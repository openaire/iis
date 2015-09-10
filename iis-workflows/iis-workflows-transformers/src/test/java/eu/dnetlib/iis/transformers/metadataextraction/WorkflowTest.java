package eu.dnetlib.iis.transformers.metadataextraction;

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
    public void testChecksumPreprocessing() throws Exception {
        runWorkflow("eu/dnetlib/iis/transformers/metadataextraction/checksum/preprocessing/sampledataproducer/oozie_app");
    }
    
    @Test
    public void testChecksumPostprocessingText() throws Exception {
        runWorkflow("eu/dnetlib/iis/transformers/metadataextraction/checksum/postprocessing/text/sampledataproducer/oozie_app");
    }
    
    @Test
    public void testChecksumPostprocessingMeta() throws Exception {
        runWorkflow("eu/dnetlib/iis/transformers/metadataextraction/checksum/postprocessing/meta/sampledataproducer/oozie_app");
    }
}
