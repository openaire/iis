package eu.dnetlib.iis.wf.transformers.common.citations.from.referencemetadata;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * @author madryk
 */
@IntegrationTest
public class CitationsFromReferenceMetadataWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testWorkflow() throws Exception {
        
        testWorkflow("eu/dnetlib/iis/wf/transformers/common/citations/from/referencemetadata/test");
    }
}
