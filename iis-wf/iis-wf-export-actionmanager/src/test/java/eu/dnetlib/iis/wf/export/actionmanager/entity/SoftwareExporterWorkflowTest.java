package eu.dnetlib.iis.wf.export.actionmanager.entity;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * @author mhorst
 */
@Category(IntegrationTest.class)
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
