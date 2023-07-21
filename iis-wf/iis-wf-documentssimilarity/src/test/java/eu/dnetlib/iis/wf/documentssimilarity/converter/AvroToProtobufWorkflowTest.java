package eu.dnetlib.iis.wf.documentssimilarity.converter;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 */
public class AvroToProtobufWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testDefaultWorkflow() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/documentssimilarity/avro_to_protobuf/sampletest");
    }

}
