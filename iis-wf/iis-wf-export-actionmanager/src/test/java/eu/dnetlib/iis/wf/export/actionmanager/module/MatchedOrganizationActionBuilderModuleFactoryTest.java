package eu.dnetlib.iis.wf.export.actionmanager.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultOrganizationProtos.ResultOrganization.Affiliation;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import eu.dnetlib.iis.wf.export.actionmanager.module.MatchedOrganizationActionBuilderModuleFactory.MatchedOrganizationActionBuilderModule;

/**
 * @author mhorst
 *
 */
public class MatchedOrganizationActionBuilderModuleFactoryTest {

    private MatchedOrganizationActionBuilderModuleFactory factory;

    private Float trustLevelThreshold = 0.5f;

    private String actionSetId = "someActionSetId";

    private Agent agent = new Agent("agentId", "agent name", Agent.AGENT_TYPE.service);

    private String docId = "documentId";

    private String orgId = "organizationId";

    private float matchStrength = 0.9f;

    private MatchedOrganization matchedOrg = buildMatchedOrganization(docId, orgId, matchStrength);

    @Before
    public void initFactory() {
        factory = new MatchedOrganizationActionBuilderModuleFactory();
    }

    // ----------------------- TESTS --------------------------

    @Test(expected = NullPointerException.class)
    public void test_constructor_null_agent() throws Exception {
        // execute
        factory.new MatchedOrganizationActionBuilderModule(trustLevelThreshold, null, actionSetId);
    }

    @Test(expected = NullPointerException.class)
    public void test_constructor_null_actionsetid() throws Exception {
        // execute
        factory.new MatchedOrganizationActionBuilderModule(trustLevelThreshold, agent, null);
    }

    @Test(expected = NullPointerException.class)
    public void test_build_null_object() throws Exception {
        // given
        MatchedOrganizationActionBuilderModule module = factory.new MatchedOrganizationActionBuilderModule(trustLevelThreshold, agent, actionSetId);
        // execute
        module.build(null);
    }
    
    @Test(expected = TrustLevelThresholdExceededException.class)
    public void test_build_below_threshold() throws Exception {
        // given
        MatchedOrganization matchedOrgBelowThreshold = buildMatchedOrganization(docId, orgId, 0.4f);
        MatchedOrganizationActionBuilderModule module = factory.new MatchedOrganizationActionBuilderModule(trustLevelThreshold, agent, actionSetId);
        // execute
        module.build(matchedOrgBelowThreshold);
    }

    @Test
    public void test_build() throws Exception {
        // given
        MatchedOrganizationActionBuilderModule module = factory.new MatchedOrganizationActionBuilderModule(trustLevelThreshold, agent, actionSetId);
        // execute
        List<AtomicAction> actions = module.build(matchedOrg);
        // assert
        assertNotNull(actions);
        assertEquals(1, actions.size());
        AtomicAction action = actions.get(0);
        assertNotNull(action);
        assertNotNull(action.getRowKey());
        assertEquals(actionSetId, action.getRawSet());
        assertEquals(orgId, action.getTargetColumn());
        assertEquals(docId, action.getTargetRowKey());
        assertEquals(RelType.resultOrganization.toString() + '_' + SubRelType.affiliation + '_'
                + Affiliation.RelName.isAffiliatedWith, action.getTargetColumnFamily());
        assertOaf(action.getTargetValue(), module.getConfidenceToTrustLevelNormalizationFactor());
    }

    // ----------------------- PRIVATE --------------------------

    private static MatchedOrganization buildMatchedOrganization(String docId, String orgId, float matchStrength) {
        MatchedOrganization.Builder builder = MatchedOrganization.newBuilder();
        builder.setDocumentId(docId);
        builder.setOrganizationId(orgId);
        builder.setMatchStrength(matchStrength);
        return builder.build();
    }

    private void assertOaf(byte[] oafBytes, float normalizationFactory) throws InvalidProtocolBufferException {
        assertNotNull(oafBytes);
        Oaf.Builder oafBuilder = Oaf.newBuilder();
        oafBuilder.mergeFrom(oafBytes);
        Oaf oaf = oafBuilder.build();
        assertNotNull(oaf);

        assertTrue(KindProtos.Kind.relation == oaf.getKind());
        assertTrue(RelType.resultOrganization == oaf.getRel().getRelType());
        assertTrue(SubRelType.affiliation == oaf.getRel().getSubRelType());
        assertEquals(Affiliation.RelName.isAffiliatedWith.toString(), oaf.getRel().getRelClass());
        assertEquals(docId, oaf.getRel().getSource());
        assertEquals(orgId, oaf.getRel().getTarget());

        assertNotNull(oaf.getDataInfo());

        float normalizedTrust = matchStrength * normalizationFactory;
        assertEquals(normalizedTrust, Float.parseFloat(oaf.getDataInfo().getTrust()), 0.0001);
    }
}
