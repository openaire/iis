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
import eu.dnetlib.data.proto.ResultOrganizationProtos.ResultOrganization.Affiliation;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.Expectations;

/**
 * @author mhorst
 *
 */
public class MatchedOrganizationActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<MatchedOrganization> {


    // ----------------------- CONSTRUCTORS --------------------------

    public MatchedOrganizationActionBuilderModuleFactoryTest() throws Exception {
        super(MatchedOrganizationActionBuilderModuleFactory.class, AlgorithmName.document_affiliations);
    }

    // ----------------------- TESTS ---------------------------------
    
    @Test(expected = TrustLevelThresholdExceededException.class)
    public void testBuildBelowThreshold() throws Exception {
        // given
        MatchedOrganization matchedOrgBelowThreshold = buildMatchedOrganization("documentId", "organizationId", 0.4f);
        ActionBuilderModule<MatchedOrganization> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        module.build(matchedOrgBelowThreshold);
    }

    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "documentId";
        String orgId = "organizationId";
        float matchStrength = 0.9f;
        ActionBuilderModule<MatchedOrganization> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> actions = module.build(buildMatchedOrganization(docId, orgId, matchStrength));
        
        // assert
        assertNotNull(actions);
        assertEquals(2, actions.size());
        AtomicAction action = actions.get(0);
        assertNotNull(action);
        assertNotNull(action.getRowKey());
        assertEquals(actionSetId, action.getRawSet());
        assertEquals(orgId, action.getTargetColumn());
        assertEquals(docId, action.getTargetRowKey());
        assertEquals(RelType.resultOrganization.toString() + '_' + SubRelType.affiliation + '_'
                + Affiliation.RelName.hasAuthorInstitution, action.getTargetColumnFamily());
        
        Expectations expectations = new Expectations(docId, orgId, matchStrength, 
                KindProtos.Kind.relation, RelType.resultOrganization, SubRelType.affiliation, 
                Affiliation.RelName.hasAuthorInstitution.toString());
        assertOafRel(action.getTargetValue(), expectations);
//      checking backward relation
        action = actions.get(1);
        assertNotNull(action);
        assertNotNull(action.getRowKey());
        assertEquals(agent, action.getAgent());
        assertEquals(actionSetId, action.getRawSet());
        assertEquals(docId, action.getTargetColumn());
        assertEquals(orgId, action.getTargetRowKey());
        assertEquals(RelType.resultOrganization.toString() + '_' + SubRelType.affiliation + '_'
                + Affiliation.RelName.isAuthorInstitutionOf, action.getTargetColumnFamily());
        expectations.setSource(orgId);
        expectations.setTarget(docId);
        expectations.setRelationClass(Affiliation.RelName.isAuthorInstitutionOf.toString());
        assertOafRel(action.getTargetValue(), expectations);
    }
    
    // ----------------------- PRIVATE --------------------------

    private static MatchedOrganization buildMatchedOrganization(String docId, String orgId, float matchStrength) {
        MatchedOrganization.Builder builder = MatchedOrganization.newBuilder();
        builder.setDocumentId(docId);
        builder.setOrganizationId(orgId);
        builder.setMatchStrength(matchStrength);
        return builder.build();
    }

}
