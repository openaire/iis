package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.Test;

public class PatentExporterWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testExportPatent() {
        testWorkflow("eu/dnetlib/iis/wf/export/actionmanager/entity/patent/default");
    }

}
