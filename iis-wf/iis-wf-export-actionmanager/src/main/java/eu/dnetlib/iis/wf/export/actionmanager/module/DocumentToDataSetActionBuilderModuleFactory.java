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
import eu.dnetlib.data.proto.ResultResultProtos.ResultResult;
import eu.dnetlib.data.proto.ResultResultProtos.ResultResult.PublicationDataset;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.referenceextraction.dataset.schemas.DocumentToDataSet;

/**
 * {@link DocumentToDataSet} based action builder module.
 * @author mhorst
 *
 */
public class DocumentToDataSetActionBuilderModuleFactory 
		implements ActionBuilderFactory<DocumentToDataSet> {

	public static final String REL_CLASS_ISRELATEDTO = PublicationDataset.RelName.isRelatedTo.toString();
	
	private static final AlgorithmName algorithmName = AlgorithmName.document_referencedDatasets;
	
	class DocumentToDataSetActionBuilderModule extends AbstractBuilderModule
	implements ActionBuilderModule<DocumentToDataSet> {
	
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 * @param trustLevelThreshold
		 */
		public DocumentToDataSetActionBuilderModule(
				String predefinedTrust, Float trustLevelThreshold) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
		}
	
		@Override
		public List<AtomicAction> build(DocumentToDataSet object, Agent agent,
				String actionSetId) throws TrustLevelThresholdExceededException {
			String docId = object.getDocumentId().toString();
			String currentRefId = object.getDatasetId().toString();
			Oaf.Builder oafBuilder = Oaf.newBuilder();
			oafBuilder.setKind(Kind.relation);
			OafRel.Builder relBuilder = OafRel.newBuilder();
			relBuilder.setChild(false);
			relBuilder.setRelType(RelType.resultResult);
			relBuilder.setSubRelType(SubRelType.publicationDataset);
			relBuilder.setRelClass(REL_CLASS_ISRELATEDTO);
			relBuilder.setSource(docId);
			relBuilder.setTarget(currentRefId);
			ResultResult.Builder resResultBuilder = ResultResult.newBuilder();
			PublicationDataset.Builder pubDatasetBuilder = PublicationDataset.newBuilder();
			pubDatasetBuilder.setRelMetadata(buildRelMetadata(
					HBaseConstants.SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_RESULT, 
					REL_CLASS_ISRELATEDTO));
			resResultBuilder.setPublicationDataset(pubDatasetBuilder.build());
			relBuilder.setResultResult(resResultBuilder.build());
			oafBuilder.setRel(relBuilder.build());
			oafBuilder.setDataInfo(object.getConfidenceLevel()!=null?
					buildInference(object.getConfidenceLevel()):
					buildInference());
			oafBuilder.setLastupdatetimestamp(System.currentTimeMillis());
			Oaf oaf = oafBuilder.build();
			Oaf oafInverted = invertBidirectionalRelationAndBuild(oafBuilder);
			return Arrays.asList(new AtomicAction[] {
					actionFactory.createAtomicAction(
						actionSetId, agent, 
						docId, 
						OafDecoder.decode(oaf).getCFQ(), 
						currentRefId, 
						oaf.toByteArray()),
	//				setting reverse relation in referenced object
					actionFactory.createAtomicAction(
						actionSetId, agent, 
						currentRefId, 
						OafDecoder.decode(oafInverted).getCFQ(),
						docId,
						oafInverted.toByteArray())
			});
		}
	}

	@Override
	public ActionBuilderModule<DocumentToDataSet> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		return new DocumentToDataSetActionBuilderModule(
				predefinedTrust, trustLevelThreshold);
	}
	
	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}
}
