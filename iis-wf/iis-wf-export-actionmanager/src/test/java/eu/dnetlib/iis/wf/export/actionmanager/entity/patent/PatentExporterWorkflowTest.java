package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class PatentExporterWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testExportPatent() {
        testWorkflow("eu/dnetlib/iis/wf/export/actionmanager/entity/patent/default");
    }

}
