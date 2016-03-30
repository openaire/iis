package eu.dnetlib.iis.wf.transformers.metadataextraction;

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
    public void testChecksumPreprocessing() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/transformers/metadataextraction/checksum/preprocessing/sampledataproducer");
    }
    
    @Test
    public void testChecksumPostprocessingMeta() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/transformers/metadataextraction/checksum/postprocessing/meta/sampledataproducer");
    }
    
    @Test
    public void testToDocumentTextConversion() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/transformers/metadataextraction/documenttext/sampledataproducer");
    }
    
    @Test
    public void testSkipExtractedWithoutMeta() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/transformers/metadataextraction/skip_extracted_without_meta/sampledataproducer");
    }
}
