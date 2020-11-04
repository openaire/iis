package eu.dnetlib.iis.wf.transformers.common.citations.from.referencemetadata;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author madryk
 */
public class CitationsFromReferenceMetadataWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testWorkflow() throws Exception {
        
        testWorkflow("eu/dnetlib/iis/wf/transformers/common/citations/from/referencemetadata/test");
    }
}
