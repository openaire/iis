package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.mapreduce.util.OafDecoder;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultOrganizationProtos.ResultOrganization;
import eu.dnetlib.data.proto.ResultOrganizationProtos.ResultOrganization.Affiliation;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;

/**
 * {@link MatchedOrganization} action builder factory module.
 * 
 * @author mhorst
 *
 */
public class MatchedOrganizationActionBuilderModuleFactory extends AbstractActionBuilderFactory<MatchedOrganization> {

    private static final String REL_CLASS_HAS_AUTHOR_INSTITUTION_OF = Affiliation.RelName.hasAuthorInstitution.toString();
    
    private static final String REL_CLASS_IS_AUTHOR_INSTITUTION_OF = Affiliation.RelName.isAuthorInstitutionOf.toString();

    private static final String SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_ORG = "dnet:result_organization_relations";

    // ------------------------ CONSTRUCTORS --------------------------
    
    public MatchedOrganizationActionBuilderModuleFactory() {
        super(AlgorithmName.document_affiliations);
    }

    // ------------------------ LOGIC ---------------------------------
    
    @Override
    public ActionBuilderModule<MatchedOrganization> instantiate(Configuration config, Agent agent, String actionSetId) {
        return new MatchedOrganizationActionBuilderModule(provideTrustLevelThreshold(config), agent, actionSetId);
    }
    
    // ------------------------ INNER CLASS ---------------------------------
    
    /**
     * {@link MatchedOrganization} action builder module.
     *
     */
    class MatchedOrganizationActionBuilderModule extends AbstractBuilderModule<MatchedOrganization> {

        
        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param agent action manager agent details
         * @param actionSetId action set identifier
         */
        public MatchedOrganizationActionBuilderModule(Float trustLevelThreshold, Agent agent, String actionSetId) {
            super(trustLevelThreshold, buildInferenceProvenance(), Preconditions.checkNotNull(agent),
                    Preconditions.checkNotNull(actionSetId));
        }

        // ------------------------ LOGIC ---------------------------------
        
        @Override
        public List<AtomicAction> build(MatchedOrganization object) throws TrustLevelThresholdExceededException {
            Preconditions.checkNotNull(object);
            String docId = object.getDocumentId().toString();
            String orgId = object.getOrganizationId().toString();
            Oaf.Builder oafBuilder = Oaf.newBuilder();
            oafBuilder.setKind(Kind.relation);
            OafRel.Builder relBuilder = OafRel.newBuilder();
            relBuilder.setChild(false);
            relBuilder.setRelType(RelType.resultOrganization);
            relBuilder.setSubRelType(SubRelType.affiliation);
            relBuilder.setRelClass(REL_CLASS_HAS_AUTHOR_INSTITUTION_OF);
            relBuilder.setSource(docId);
            relBuilder.setTarget(orgId);
            ResultOrganization.Builder resOrgBuilder = ResultOrganization.newBuilder();
            Affiliation.Builder affBuilder = Affiliation.newBuilder();
            affBuilder.setRelMetadata(
                    buildRelMetadata(SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_ORG, REL_CLASS_HAS_AUTHOR_INSTITUTION_OF));
            resOrgBuilder.setAffiliation(affBuilder.build());
            relBuilder.setResultOrganization(resOrgBuilder.build());
            oafBuilder.setRel(relBuilder.build());
            oafBuilder.setDataInfo(buildInference(object.getMatchStrength()));
            oafBuilder.setLastupdatetimestamp(System.currentTimeMillis());
            Oaf oaf = oafBuilder.build();
            Oaf oafInverted = invertRelationAndBuild(oafBuilder);
            return Arrays.asList(new AtomicAction[] {
                    getActionFactory().createAtomicAction(getActionSetId(), getAgent(), docId,
                            OafDecoder.decode(oaf).getCFQ(), orgId, oaf.toByteArray()),
                 // setting reverse relation in referenced object
                    getActionFactory().createAtomicAction(getActionSetId(), getAgent(), orgId,
                            OafDecoder.decode(oafInverted).getCFQ(), docId, oafInverted.toByteArray())
                    });
        }

        // ------------------------ PRIVATE ---------------------------------
        
        /**
         * Clones builder provided as parameter, inverts relations and builds {@link Oaf} object.
         */
        private Oaf invertRelationAndBuild(Oaf.Builder existingBuilder) {
            // works on builder clone to prevent changes in existing builder
            if (existingBuilder.getRel() != null) {
                if (existingBuilder.getRel().getSource() != null && existingBuilder.getRel().getTarget() != null) {
                    Oaf.Builder builder = existingBuilder.clone();
                    OafRel.Builder relBuilder = builder.getRelBuilder();
                    String source = relBuilder.getSource();
                    String target = relBuilder.getTarget();
                    relBuilder.setSource(target);
                    relBuilder.setTarget(source);
                    relBuilder.setRelClass(REL_CLASS_IS_AUTHOR_INSTITUTION_OF);
                    relBuilder.getResultOrganizationBuilder().getAffiliationBuilder().getRelMetadataBuilder()
                            .getSemanticsBuilder().setClassid(REL_CLASS_IS_AUTHOR_INSTITUTION_OF);
                    relBuilder.getResultOrganizationBuilder().getAffiliationBuilder().getRelMetadataBuilder()
                            .getSemanticsBuilder().setClassname(REL_CLASS_IS_AUTHOR_INSTITUTION_OF);
                    builder.setRel(relBuilder.build());
                    builder.setLastupdatetimestamp(System.currentTimeMillis());
                    return builder.build();
                } else {
                    throw new RuntimeException("invalid state: " + "either source or target relation was missing!");
                }
            } else {
                throw new RuntimeException("invalid state: " + "no relation object found!");
            }
        }
    }
}
