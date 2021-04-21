package eu.dnetlib.iis.wf.export.actionmanager.entity.software;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

/**
 * @author mhorst
 */
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
