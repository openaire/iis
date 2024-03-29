package eu.dnetlib.iis.wf.primary.main;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class WorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    @DisplayName("Primary main workflow fails on output without write permissions")
    public void givenPrimaryMainWorkflow_whenExecutedOnOutputWithoutWritePermission_thenWorkflowFails() {
        testWorkflow("eu/dnetlib/iis/wf/primary/main/output-validation");
    }
}
