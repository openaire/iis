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
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganizationWithProvenance;
import eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.Expectations;

/**
 * @author mhorst
 *
 */
public class MatchedOrganizationActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<MatchedOrganizationWithProvenance> {


    // ----------------------- CONSTRUCTORS --------------------------

    public MatchedOrganizationActionBuilderModuleFactoryTest() throws Exception {
        super(MatchedOrganizationActionBuilderModuleFactory.class, AlgorithmName.document_affiliations);
    }

    // ----------------------- TESTS ---------------------------------
    
    @Test(expected = TrustLevelThresholdExceededException.class)
    public void testBuildBelowThreshold() throws Exception {
        // given
        MatchedOrganizationWithProvenance matchedOrgBelowThreshold = buildMatchedOrganization("documentId", "organizationId", 0.4f, "affmatch");
        ActionBuilderModule<MatchedOrganizationWithProvenance> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        module.build(matchedOrgBelowThreshold);
    }

    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "documentId";
        String orgId = "organizationId";
        float matchStrength = 0.9f;
        String provenance = "affmatch";
        ActionBuilderModule<MatchedOrganizationWithProvenance> module = factory.instantiate(config, agent, actionSetId);
        
        // execute
        List<AtomicAction> actions = module.build(buildMatchedOrganization(docId, orgId, matchStrength, provenance));
        
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

    private static MatchedOrganizationWithProvenance buildMatchedOrganization(String docId, String orgId, float matchStrength,
            String provenance) {
        MatchedOrganizationWithProvenance.Builder builder = MatchedOrganizationWithProvenance.newBuilder();
        builder.setDocumentId(docId);
        builder.setOrganizationId(orgId);
        builder.setMatchStrength(matchStrength);
        builder.setProvenance(provenance);
        return builder.build();
    }

}
