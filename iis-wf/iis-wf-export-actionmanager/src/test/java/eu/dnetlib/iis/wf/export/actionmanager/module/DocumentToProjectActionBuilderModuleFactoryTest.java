package eu.dnetlib.iis.wf.export.actionmanager.module;

import static eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.assertOafRel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Test;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultProjectProtos.ResultProject.Outcome;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.Expectations;

/**
 * @author mhorst
 *
 */
public class DocumentToProjectActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<DocumentToProject> {

    // ----------------------- CONSTRUCTORS --------------------------

    public DocumentToProjectActionBuilderModuleFactoryTest() throws Exception {
        super(DocumentToProjectActionBuilderModuleFactory.class, AlgorithmName.document_referencedProjects);
    }

    // ----------------------- TESTS ---------------------------------
    
    @Test(expected = TrustLevelThresholdExceededException.class)
    public void testBuildBelowThreshold() throws Exception {
        // given
        DocumentToProject documentToProject = buildDocumentToProject("documentId", "projectId", 0.4f);
        ActionBuilderModule<DocumentToProject> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        module.build(documentToProject);
    }

    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "documentId";
        String projectId = "projectId";
        float matchStrength = 0.9f;
        ActionBuilderModule<DocumentToProject> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> actions = module.build(buildDocumentToProject(docId, projectId, matchStrength));
        
        // assert
        assertNotNull(actions);
        assertEquals(2, actions.size());
        AtomicAction action = actions.get(0);
        assertNotNull(action);
        assertNotNull(action.getRowKey());
        assertEquals(actionSetId, action.getRawSet());
        assertEquals(projectId, action.getTargetColumn());
        assertEquals(docId, action.getTargetRowKey());
        assertEquals(RelType.resultProject.toString() + '_' + SubRelType.outcome + '_'
                + Outcome.RelName.isProducedBy, action.getTargetColumnFamily());
        
        Expectations expectations = new Expectations(docId, projectId, matchStrength, 
                KindProtos.Kind.relation, RelType.resultProject, SubRelType.outcome, 
                Outcome.RelName.isProducedBy.toString());
        assertOafRel(action.getTargetValue(), expectations);
        
//      checking backward relation
        action = actions.get(1);
        assertNotNull(action);
        assertNotNull(action.getRowKey());
        assertEquals(agent, action.getAgent());
        assertEquals(actionSetId, action.getRawSet());
        assertEquals(docId, action.getTargetColumn());
        assertEquals(projectId, action.getTargetRowKey());
        assertEquals(RelType.resultProject.toString() + '_' + SubRelType.outcome + '_'
                + Outcome.RelName.produces, action.getTargetColumnFamily());
        expectations.setSource(projectId);
        expectations.setTarget(docId);
        expectations.setRelationClass(Outcome.RelName.produces.toString());
        assertOafRel(action.getTargetValue(), expectations);
    }
    
    // ----------------------- PRIVATE --------------------------

    private static DocumentToProject buildDocumentToProject(String docId, String projectId, 
            float confidenceLevel) {
        DocumentToProject.Builder builder = DocumentToProject.newBuilder();
        builder.setDocumentId(docId);
        builder.setProjectId(projectId);
        builder.setConfidenceLevel(confidenceLevel);
        return builder.build();
    }

}
