package eu.dnetlib.iis.wf.export.actionmanager.entity;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 */
@IntegrationTest
public class SoftwareExporterWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testExportSoftware() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/export/actionmanager/entity/software/default");
    }

    @Test
    public void testExportSoftwareHeritage() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/export/actionmanager/entity/software/heritage");
    }
}
