package eu.dnetlib.iis.wf.export.actionmanager.entity;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;

/**
 * @author mhorst
 */
@Category(IntegrationTest.class)
public class EntityExportWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testExportDatasetEntity() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/export/actionmanager/entity/dataset/default");
    }
    
    @Test
    public void testExportDocumentEntity() throws Exception {
        testWorkflow("eu/dnetlib/iis/wf/export/actionmanager/entity/document/default");
    }

}
