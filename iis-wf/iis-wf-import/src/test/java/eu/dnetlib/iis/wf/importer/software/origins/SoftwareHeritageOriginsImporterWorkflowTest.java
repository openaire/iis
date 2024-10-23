package eu.dnetlib.iis.wf.importer.software.origins;

import org.junit.jupiter.api.Test;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.OozieWorkflowTestConfiguration;

/**
 * {@link SoftwareHeritageOriginsImporterJob} integration test involving oozie workflow definition.
 * @author mhorst
 */
public class SoftwareHeritageOriginsImporterWorkflowTest extends AbstractOozieWorkflowTestCase  {

    
    @Test
    public void testImportOriginsFromOrc() {
        OozieWorkflowTestConfiguration wfConf = new OozieWorkflowTestConfiguration();
        wfConf.setTimeoutInSeconds(720);
        testWorkflow("eu/dnetlib/iis/wf/importer/software_origins/sampletest", wfConf);
    }
}
