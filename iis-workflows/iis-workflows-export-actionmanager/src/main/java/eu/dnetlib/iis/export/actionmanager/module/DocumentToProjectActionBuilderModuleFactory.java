package eu.dnetlib.iis.export.actionmanager.module;

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

/**
 * {@link DocumentToProject} based action builder module.
 * @author mhorst
 *
 */
public class DocumentToProjectActionBuilderModuleFactory 
	implements ActionBuilderFactory<DocumentToProject> {

	public static final String REL_CLASS_ISPRODUCEDBY = Outcome.RelName.isProducedBy.toString();
	
	public static final String REL_CLASS_PRODUCES = Outcome.RelName.produces.toString();

	private static final AlgorithmName algorithmName = AlgorithmName.document_referencedProjects;
	
	class DocumentToProjectActionBuilderModule extends AbstractBuilderModule
	implements ActionBuilderModule<DocumentToProject> {
	
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 * @param trustLevelThreshold
		 */
		public DocumentToProjectActionBuilderModule(
				String predefinedTrust, Float trustLevelThreshold) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
		}
	
		@Override
		public List<AtomicAction> build(DocumentToProject object, 
				Agent agent, String actionSetId) throws TrustLevelThresholdExceededException {
			String docId = object.getDocumentId().toString();
			String currentProjectIdStr = object.getProjectId().toString();
			Oaf.Builder oafBuilder = Oaf.newBuilder();
			oafBuilder.setKind(Kind.relation);
			OafRel.Builder relBuilder = OafRel.newBuilder();
			relBuilder.setChild(false);
			relBuilder.setRelType(RelType.resultProject);
			relBuilder.setSubRelType(SubRelType.outcome);
			relBuilder.setRelClass(REL_CLASS_ISPRODUCEDBY);
			relBuilder.setSource(docId);
			relBuilder.setTarget(currentProjectIdStr);
			ResultProject.Builder resProjBuilder = ResultProject.newBuilder();
			Outcome.Builder outcomeBuilder = Outcome.newBuilder();
			outcomeBuilder.setRelMetadata(buildRelMetadata(
					HBaseConstants.SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_PROJECT, 
					REL_CLASS_ISPRODUCEDBY));
			resProjBuilder.setOutcome(outcomeBuilder.build());
			relBuilder.setResultProject(resProjBuilder.build());
			oafBuilder.setRel(relBuilder.build());
			oafBuilder.setDataInfo(object.getConfidenceLevel()!=null?
							buildInference(object.getConfidenceLevel()):
							buildInference());
			oafBuilder.setTimestamp(System.currentTimeMillis());
			Oaf oaf = oafBuilder.build();
			Oaf oafInv = invertRelationAndBuild(oafBuilder);
			return Arrays.asList(new AtomicAction[] {
					actionFactory.createAtomicAction(
						actionSetId, agent, 
						docId, 
						OafDecoder.decode(oaf).getCFQ(), 
						currentProjectIdStr, 
						oaf.toByteArray()),
//				setting reverse relation in project object
				actionFactory.createAtomicAction(
						actionSetId, agent, 
						currentProjectIdStr, 
						OafDecoder.decode(oafInv).getCFQ(), 
						docId, 
						oafInv.toByteArray())
			});
		}

		/**
		 * Clones builder provided as parameter, inverts relations and builds Oaf object.
		 * @param existingBuilder
		 * @param invertedClassName
		 * @return Oaf object containing relation with inverted source and target fields and inverted relation direction
		 */
		private Oaf invertRelationAndBuild(Oaf.Builder existingBuilder) {
//			works on builder clone to prevent changes in existing builder
			if (existingBuilder.getRel()!=null) {
				if (existingBuilder.getRel().getSource()!=null &&
						existingBuilder.getRel().getTarget()!=null) {
					Oaf.Builder builder = existingBuilder.clone();
					OafRel.Builder relBuilder = builder.getRelBuilder();
					String source = relBuilder.getSource();
					String target = relBuilder.getTarget();
					relBuilder.setSource(target);
					relBuilder.setTarget(source);
					relBuilder.setRelClass(REL_CLASS_PRODUCES);
					if (relBuilder.getResultProjectBuilder()!=null && 
							relBuilder.getResultProjectBuilder().getOutcomeBuilder()!=null &&
							relBuilder.getResultProjectBuilder().getOutcomeBuilder().getRelMetadataBuilder()!=null &&
							relBuilder.getResultProjectBuilder().getOutcomeBuilder().getRelMetadataBuilder().getSemanticsBuilder()!=null) {
						relBuilder.getResultProjectBuilder().getOutcomeBuilder().getRelMetadataBuilder().getSemanticsBuilder().setClassid(REL_CLASS_PRODUCES);
						relBuilder.getResultProjectBuilder().getOutcomeBuilder().getRelMetadataBuilder().getSemanticsBuilder().setClassname(REL_CLASS_PRODUCES);
					}
					builder.setRel(relBuilder.build());
					builder.setTimestamp(System.currentTimeMillis());
					return builder.build();
				} else {
					throw new RuntimeException("invalid state: " +
							"either source or target relation was missing!");
				}
			} else {
				throw new RuntimeException("invalid state: " +
						"no relation object found!");
			}
		}
	}

	@Override
	public ActionBuilderModule<DocumentToProject> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		return new DocumentToProjectActionBuilderModule(
				predefinedTrust, trustLevelThreshold);
	}
	
	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}
}
