package eu.dnetlib.iis.workflows.transformers.export.documenttoproject_without_imported_data;

import eu.dnetlib.iis.IntegrationTest;
import eu.dnetlib.iis.core.AbstractWorkflowTestCase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * 
 * @author Dominika Tkaczyk
 *
 */
@Category(IntegrationTest.class)
public class WorkflowTest extends AbstractWorkflowTestCase {

    @Test
	public void testWorkflow() throws Exception {
        runWorkflow("eu/dnetlib/iis/transformers/export/documenttoproject_without_imported_data/sampledataproducer/oozie_app");
    }

}
