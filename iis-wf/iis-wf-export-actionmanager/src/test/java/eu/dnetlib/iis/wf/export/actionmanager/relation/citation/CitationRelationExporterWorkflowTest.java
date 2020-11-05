package eu.dnetlib.iis.wf.export.actionmanager.relation.citation;

import eu.dnetlib.iis.common.AbstractOozieWorkflowTestCase;
import eu.dnetlib.iis.common.IntegrationTest;
import org.junit.jupiter.api.Test;

@IntegrationTest
public class CitationRelationExporterWorkflowTest extends AbstractOozieWorkflowTestCase {

    @Test
    public void testExportCitationRelation() {
        testWorkflow("eu/dnetlib/iis/wf/export/actionmanager/relation/citation/default");
    }
}
