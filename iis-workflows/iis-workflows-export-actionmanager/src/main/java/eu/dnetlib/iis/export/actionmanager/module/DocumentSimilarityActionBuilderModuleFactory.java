package eu.dnetlib.iis.export.actionmanager.module;

import static eu.dnetlib.iis.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_DOCUMENTSSIMILARITY_THRESHOLD;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.mapreduce.util.OafDecoder;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultResultProtos.ResultResult;
import eu.dnetlib.data.proto.ResultResultProtos.ResultResult.Similarity;
import eu.dnetlib.data.proto.ResultResultProtos.ResultResult.Similarity.Type;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.documentssimilarity.schemas.DocumentSimilarity;

/**
 * {@link DocumentSimilarity} based action builder module.
 * @author mhorst
 *
 */
public class DocumentSimilarityActionBuilderModuleFactory  
	implements ActionBuilderFactory<DocumentSimilarity> {

	private static final AlgorithmName algorithmName = AlgorithmName.document_similarities_standard;
	
	private final Logger log = Logger.getLogger(this.getClass());
	
	class DocumentSimilarityActionBuilderModule extends AbstractBuilderModule 
	implements ActionBuilderModule<DocumentSimilarity> {
	
		private final Float similarityThreshold;
		
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 * @param threshold similarity threshold, skipped when null
		 */
		public DocumentSimilarityActionBuilderModule(
				String predefinedTrust, Float trustLevelThreshold,
				Float similarityThreshold) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
			this.similarityThreshold = similarityThreshold;
		}
		
		@Override
		public List<AtomicAction> build(DocumentSimilarity object, Agent agent,
				String actionSetId) {
			if (object==null) {
				return Collections.emptyList();
			}
//			checking similarity threshold if set
			if (similarityThreshold!=null && object.getSimilarity()!=null &&
					object.getSimilarity()<similarityThreshold) {
				return Collections.emptyList();
			}
//			setting relations in both source and target objects
			List<AtomicAction> simActions = createActions(
					object, actionSetId, agent, false);
			List<AtomicAction> reverseSimActions = createActions(
					object, actionSetId, agent, true);
			List<AtomicAction> results = new ArrayList<AtomicAction>();
			if (simActions!=null && !simActions.isEmpty()) {
				results.addAll(simActions);
			}
			if (reverseSimActions!=null && !reverseSimActions.isEmpty()) {
				results.addAll(reverseSimActions);
			}
			return results;
		}
		
		/**
		 * Creates similarity related puts.
		 * @param object
		 * @param actionSet
		 * @param agent
		 * @param backwardMode
		 * @return similarity related puts
		 */
		protected List<AtomicAction> createActions(DocumentSimilarity object, 
				String actionSet, Agent agent, boolean backwardMode) {
			Oaf oafObjectRel = buildOAFRel(
					object.getDocumentId().toString(), 
					object.getOtherDocumentId().toString(),
					object.getSimilarity(), backwardMode);
			if (oafObjectRel==null) {
				return Collections.emptyList();
			}
			List<AtomicAction> actionList = new ArrayList<AtomicAction>();
			AtomicAction currentAction = actionFactory.createAtomicAction(
					actionSet, agent, 
					backwardMode?
							object.getOtherDocumentId().toString():
								object.getDocumentId().toString(), 
					OafDecoder.decode(oafObjectRel).getCFQ(), 
					backwardMode?
							object.getDocumentId().toString():
								object.getOtherDocumentId().toString(), 
					oafObjectRel.toByteArray());
			actionList.add(currentAction);
			return actionList;
		}
		
		/**
		 * Builds OAF object.
		 * @param source
		 * @param target
		 * @param score
		 * @param invert flag indicating source and target should be inverted
		 * @return OAF object
		 */
		private Oaf buildOAFRel(String sourceId, String targetDocId, 
				float score, boolean invert) {
			OafRel.Builder relBuilder = OafRel.newBuilder();
			if (!invert) {
				relBuilder.setSource(sourceId);
				relBuilder.setTarget(targetDocId);
				
			} else {
				relBuilder.setSource(targetDocId);
				relBuilder.setTarget(sourceId);
			}
			String relClass = invert?
					Similarity.RelName.isAmongTopNSimilarDocuments.toString():
						Similarity.RelName.hasAmongTopNSimilarDocuments.toString();
			relBuilder.setChild(false);
			relBuilder.setRelType(RelType.resultResult);
			relBuilder.setSubRelType(SubRelType.similarity);
			relBuilder.setRelClass(relClass);
			ResultResult.Builder resultResultBuilder = ResultResult.newBuilder();
			Similarity.Builder similarityBuilder = Similarity.newBuilder();
			similarityBuilder.setRelMetadata(buildRelMetadata(
					HBaseConstants.SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_RESULT, 
					relClass));
			similarityBuilder.setSimilarity(score);
			similarityBuilder.setType(Type.STANDARD);
			resultResultBuilder.setSimilarity(similarityBuilder.build());
			relBuilder.setResultResult(resultResultBuilder.build());
			
			Oaf.Builder oafBuilder = Oaf.newBuilder();
			oafBuilder.setKind(Kind.relation);
			oafBuilder.setRel(relBuilder.build());
			oafBuilder.setDataInfo(buildInference());
			oafBuilder.setTimestamp(System.currentTimeMillis());
			return oafBuilder.build();
		}
	}

	@Override
	public ActionBuilderModule<DocumentSimilarity> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		String thresholdStr = config.get(
				EXPORT_DOCUMENTSSIMILARITY_THRESHOLD);
		Float similarityThreshold = null;
		if (thresholdStr!=null && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(
				thresholdStr)) {
			similarityThreshold = Float.valueOf(thresholdStr);
			log.warn("setting documents similarity exporter threshold to: " + similarityThreshold);
		}
		return new DocumentSimilarityActionBuilderModule(
				predefinedTrust, trustLevelThreshold, similarityThreshold);
	}
	
	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}
}
