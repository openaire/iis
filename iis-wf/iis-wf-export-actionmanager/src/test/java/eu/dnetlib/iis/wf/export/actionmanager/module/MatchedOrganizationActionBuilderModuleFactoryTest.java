package eu.dnetlib.iis.wf.export.actionmanager.module;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganizationWithProvenance;
import eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.Expectations;
import org.junit.jupiter.api.Test;

import java.util.List;

import static eu.dnetlib.iis.wf.export.actionmanager.module.VerificationUtils.assertOafRel;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author mhorst
 *
 */
public class MatchedOrganizationActionBuilderModuleFactoryTest extends AbstractActionBuilderModuleFactoryTest<MatchedOrganizationWithProvenance, Relation> {


    // ----------------------- CONSTRUCTORS --------------------------

    public MatchedOrganizationActionBuilderModuleFactoryTest() throws Exception {
        super(MatchedOrganizationActionBuilderModuleFactory.class, AlgorithmName.document_affiliations);
    }

    // ----------------------- TESTS ---------------------------------
    
    @Test
    public void testBuildBelowThreshold() {
        // given
        MatchedOrganizationWithProvenance matchedOrgBelowThreshold = buildMatchedOrganization("documentId", "organizationId", 0.4f, "affmatch");
        ActionBuilderModule<MatchedOrganizationWithProvenance, Relation> module = factory.instantiate(config);
        
        // execute
        assertThrows(TrustLevelThresholdExceededException.class, () -> module.build(matchedOrgBelowThreshold));
    }

    @Test
    public void testBuild() throws Exception {
        // given
        String docId = "documentId";
        String orgId = "organizationId";
        float matchStrength = 0.9f;
        String provenance = "affmatch";
        ActionBuilderModule<MatchedOrganizationWithProvenance, Relation> module = factory.instantiate(config);
        
        // execute
        List<AtomicAction<Relation>> actions = module.build(buildMatchedOrganization(docId, orgId, matchStrength, provenance));
        
        // assert
        assertNotNull(actions);
        assertEquals(2, actions.size());
        AtomicAction<Relation> action = actions.get(0);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        Expectations expectations = new Expectations(docId, orgId, matchStrength, 
                "resultOrganization", "affiliation", "hasAuthorInstitution");
        assertOafRel(action.getPayload(), expectations);
//      checking backward relation
        action = actions.get(1);
        assertNotNull(action);
        assertEquals(Relation.class, action.getClazz());
        expectations.setSource(orgId);
        expectations.setTarget(docId);
        expectations.setRelationClass("isAuthorInstitutionOf");
        assertOafRel(action.getPayload(), expectations);
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
