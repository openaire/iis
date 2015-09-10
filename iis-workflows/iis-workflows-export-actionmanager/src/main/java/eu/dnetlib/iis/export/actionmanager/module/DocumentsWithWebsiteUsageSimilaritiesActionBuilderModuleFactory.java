package eu.dnetlib.iis.export.actionmanager.module;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.mapreduce.util.OafDecoder;
import eu.dnetlib.data.proto.DedupSimilarityProtos.DedupSimilarity;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultResultProtos.ResultResult;
import eu.dnetlib.data.proto.ResultResultProtos.ResultResult.Similarity;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.websiteusage.schemas.DocumentsWithWebsiteUsageSimilarities;

/**
 * {@link DocumentsWithWebsiteUsageSimilarities} based action builder module.
 * @author mhorst
 *
 */
public class DocumentsWithWebsiteUsageSimilaritiesActionBuilderModuleFactory 
	implements ActionBuilderFactory<DocumentsWithWebsiteUsageSimilarities> {

	private static final AlgorithmName algorithmName = AlgorithmName.document_similarities_websiteusage;
	
	class DocumentsWithWebsiteUsageSimilaritiesActionBuilder extends
	AbstractBuilderModule implements ActionBuilderModule<DocumentsWithWebsiteUsageSimilarities> {
	
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 * @param trustLevelThreshold
		 */
		public DocumentsWithWebsiteUsageSimilaritiesActionBuilder(
				String predefinedTrust, Float trustLevelThreshold) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
		}
	
		@Override
		public List<AtomicAction> build(
				DocumentsWithWebsiteUsageSimilarities object, Agent agent, 
				String actionSetId) {
			String currentSimDocId = object.getOtherDocumentId().toString();
			String docId = object.getDocumentId().toString();
			Oaf similarityRel = buildSimilarityRel(
					docId, currentSimDocId,
					object.getCovisitedSimilarity());
			return Collections.singletonList(
					actionFactory.createAtomicAction(
						actionSetId, agent, docId, 
						OafDecoder.decode(similarityRel).getCFQ(), 
						currentSimDocId, 
						similarityRel.toByteArray()));
		}
	
		/**
		 * Builds OAF object.
		 * @param source
		 * @param target
		 * @param score
		 * @return OAF object
		 */
		private Oaf buildSimilarityRel(String source, String target, Float score) {
//			FIXME we are lacking relClass for webusage similarities
			String relClass = DedupSimilarity.RelName.isSimilarTo.toString(); 
			OafRel.Builder relBuilder = OafRel.newBuilder();
			relBuilder.setSource(source);
			relBuilder.setTarget(target);
			relBuilder.setChild(false);
			relBuilder.setRelType(RelType.resultResult);
			relBuilder.setSubRelType(SubRelType.similarity);
			relBuilder.setRelClass(relClass);
			
			ResultResult.Builder resultResultBuilder = ResultResult.newBuilder();
			Similarity.Builder similarityBuilder = Similarity.newBuilder();
			similarityBuilder.setRelMetadata(buildRelMetadata(
					HBaseConstants.SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_RESULT, 
					relClass));
			similarityBuilder.setType(ResultResult.Similarity.Type.WEBUSAGE);
			if (score!=null) {
				similarityBuilder.setSimilarity(score);
			}
			resultResultBuilder.setSimilarity(similarityBuilder.build());
			relBuilder.setResultResult(resultResultBuilder.build());
			
			eu.dnetlib.data.proto.OafProtos.Oaf.Builder oafBuilder = Oaf.newBuilder();
			oafBuilder.setKind(Kind.relation);
			oafBuilder.setRel(relBuilder.build());
			oafBuilder.setDataInfo(buildInference());
			oafBuilder.setTimestamp(System.currentTimeMillis());
			return oafBuilder.build();
		}
	}


	@Override
	public ActionBuilderModule<DocumentsWithWebsiteUsageSimilarities> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		return new DocumentsWithWebsiteUsageSimilaritiesActionBuilder(
				predefinedTrust, trustLevelThreshold);
	}
	
	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}
}
