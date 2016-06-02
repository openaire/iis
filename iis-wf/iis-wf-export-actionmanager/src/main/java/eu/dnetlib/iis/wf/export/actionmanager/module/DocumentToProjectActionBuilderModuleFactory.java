package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.mapreduce.util.OafDecoder;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultProjectProtos.ResultProject;
import eu.dnetlib.data.proto.ResultProjectProtos.ResultProject.Outcome;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;

/**
 * {@link DocumentToProject} based action builder module.
 * 
 * @author mhorst
 *
 */
public class DocumentToProjectActionBuilderModuleFactory extends AbstractBuilderFactory<DocumentToProject> {

    public static final String REL_CLASS_ISPRODUCEDBY = Outcome.RelName.isProducedBy.toString();

    public static final String REL_CLASS_PRODUCES = Outcome.RelName.produces.toString();

    // ------------------------ CONSTRUCTORS --------------------------
    
    public DocumentToProjectActionBuilderModuleFactory() {
        super(AlgorithmName.document_referencedProjects);
    }

    // ------------------------ LOGIC ----------------------------------
    
    @Override
    public ActionBuilderModule<DocumentToProject> instantiate(Configuration config, Agent agent, String actionSetId) {
        return new DocumentToProjectActionBuilderModule(provideTrustLevelThreshold(config), agent, actionSetId);
    }
    
    // ------------------------ INNER CLASS ----------------------------
    
    class DocumentToProjectActionBuilderModule extends AbstractBuilderModule<DocumentToProject> {


        // ------------------------ CONSTRUCTORS --------------------------
        
        /**
         * @param trustLevelThreshold trust level threshold or null when all records should be exported
         * @param agent action manager agent details
         * @param actionSetId action set identifier
         */
        public DocumentToProjectActionBuilderModule(Float trustLevelThreshold, Agent agent, String actionSetId) {
            super(trustLevelThreshold, buildInferenceProvenance(), agent, actionSetId);
        }
        
        // ------------------------ LOGIC ----------------------------------

        @Override
        public List<AtomicAction> build(DocumentToProject object) throws TrustLevelThresholdExceededException {
            Oaf.Builder oafBuilder = instantiateOafBuilder(object);
            Oaf oaf = oafBuilder.build();
            Oaf oafInv = invertRelationAndBuild(oafBuilder);
            return Arrays.asList(new AtomicAction[] {
                    actionFactory.createAtomicAction(actionSetId, agent, object.getDocumentId().toString(), OafDecoder.decode(oaf).getCFQ(),
                            object.getProjectId().toString(), oaf.toByteArray()),
                    // setting reverse relation in project object
                    actionFactory.createAtomicAction(actionSetId, agent, object.getProjectId().toString(),
                            OafDecoder.decode(oafInv).getCFQ(), object.getDocumentId().toString(), oafInv.toByteArray())});
        }

        // ------------------------ PRIVATE ----------------------------------

        private Oaf.Builder instantiateOafBuilder(DocumentToProject object) throws TrustLevelThresholdExceededException {
            String docId = object.getDocumentId().toString();
            String projectId = object.getProjectId().toString();
            Oaf.Builder oafBuilder = Oaf.newBuilder();
            oafBuilder.setKind(Kind.relation);
            oafBuilder.setRel(buildOafRel(docId, projectId));
            oafBuilder.setDataInfo(object.getConfidenceLevel() != null ? buildInference(object.getConfidenceLevel())
                    : buildInferenceForTrustLevel(StaticConfigurationProvider.ACTION_TRUST_0_9));
            oafBuilder.setLastupdatetimestamp(System.currentTimeMillis());
            return oafBuilder;
        }
        
        private OafRel buildOafRel(String docId, String projectId) {
            OafRel.Builder relBuilder = OafRel.newBuilder();
            relBuilder.setChild(false);
            relBuilder.setRelType(RelType.resultProject);
            relBuilder.setSubRelType(SubRelType.outcome);
            relBuilder.setRelClass(REL_CLASS_ISPRODUCEDBY);
            relBuilder.setSource(docId);
            relBuilder.setTarget(projectId);
            ResultProject.Builder resProjBuilder = ResultProject.newBuilder();
            Outcome.Builder outcomeBuilder = Outcome.newBuilder();
            outcomeBuilder.setRelMetadata(buildRelMetadata(HBaseConstants.SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_PROJECT,
                    REL_CLASS_ISPRODUCEDBY));
            resProjBuilder.setOutcome(outcomeBuilder.build());
            relBuilder.setResultProject(resProjBuilder.build());
            return relBuilder.build();
        }
        
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
                    relBuilder.setRelClass(REL_CLASS_PRODUCES);
                    if (relBuilder.getResultProjectBuilder() != null
                            && relBuilder.getResultProjectBuilder().getOutcomeBuilder() != null
                            && relBuilder.getResultProjectBuilder().getOutcomeBuilder().getRelMetadataBuilder() != null
                            && relBuilder.getResultProjectBuilder().getOutcomeBuilder().getRelMetadataBuilder()
                                    .getSemanticsBuilder() != null) {
                        relBuilder.getResultProjectBuilder().getOutcomeBuilder().getRelMetadataBuilder()
                                .getSemanticsBuilder().setClassid(REL_CLASS_PRODUCES);
                        relBuilder.getResultProjectBuilder().getOutcomeBuilder().getRelMetadataBuilder()
                                .getSemanticsBuilder().setClassname(REL_CLASS_PRODUCES);
                    }
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
