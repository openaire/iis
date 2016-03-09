package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.text.DecimalFormat;

import eu.dnetlib.actionmanager.actions.ActionFactory;
import eu.dnetlib.data.proto.FieldTypeProtos.DataInfo;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.RelMetadataProtos.RelMetadata;
import eu.dnetlib.iis.common.hbase.HBaseConstants;

/**
 * Abstract builder module.
 * @author mhorst
 *
 */
public abstract class AbstractBuilderModule {

	/**
	 * Predefined trust level.
	 */
	private final String predefinedTrust;
	
	/**
	 * Trust level threshold, null when not specified.
	 */
	private final Float trustLevelThreshold;
	
	/**
	 * Confidence to trust level normalization factor.
	 */
	private float confidenceToTrustLevelNormalizationFactor = HBaseConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
	
	/**
	 * Inference provenance.
	 */
	protected final String inferenceProvenance;
	
	/**
	 * Action factory to be used for building actions.
	 */
	protected final ActionFactory actionFactory = new ActionFactory();
	
	/**
	 * Trust level format.
	 */
	private final DecimalFormat decimalFormat = new DecimalFormat("#.####");
	
	/**
	 * Default constructor.
	 * @param predefinedTrust
	 * @param trustLevelThreshold
	 * @param inferenceProvenance
	 */
	public AbstractBuilderModule(String predefinedTrust,
			Float trustLevelThreshold,
			AlgorithmName algorithmName) {
		this.predefinedTrust = predefinedTrust;
		this.trustLevelThreshold = trustLevelThreshold;
		this.inferenceProvenance = buildInferenceProvenance(algorithmName);
	}
	
	/**
	 * Builds inference provenance string based on algorithm name.
	 * @param algorithmName
	 * @return inference provenance string based on algorithm name
	 */
	private static final String buildInferenceProvenance(AlgorithmName algorithmName) {
		return HBaseConstants.SEMANTIC_CLASS_IIS + 
				HBaseConstants.INFERENCE_PROVENANCE_SEPARATOR +
				algorithmName.toString();
	}
	
	/**
	 * Builds {@link Oaf} object.
	 * @param oafEntity
	 * @return {@link Oaf} object
	 */
	protected Oaf buildOaf(OafEntity oafEntity) {
		return buildOaf(oafEntity, null);
	}
	
	/**
	 * Builds {@link Oaf} object.
	 * @param oafEntity
	 * @param dataInfo
	 * @return {@link Oaf} object
	 */
	protected Oaf buildOaf(OafEntity oafEntity, DataInfo dataInfo) {
		eu.dnetlib.data.proto.OafProtos.Oaf.Builder oafBuilder = Oaf.newBuilder();
		oafBuilder.setKind(Kind.entity);
		oafBuilder.setEntity(oafEntity);
		if (dataInfo!=null) {
			oafBuilder.setDataInfo(dataInfo);
		}
		oafBuilder.setLastupdatetimestamp(System.currentTimeMillis());
		return oafBuilder.build();
	}
	
	/**
	 * Returns inference object.
	 * @return inference object
	 */
	protected DataInfo buildInference() {
		return buildInferenceForTrustLevel(predefinedTrust);
	}
	
	/**
	 * Returns inference object.
	 * Confidence level will be normalized to trust level.
	 * @param confidenceLevel confidence level which will be normalized to trust level
	 * @throws TrustLevelThresholdExceededException thrown when trust level threshold was exceeded
	 * @return inference object
	 */
	protected DataInfo buildInference(float confidenceLevel) throws TrustLevelThresholdExceededException {
		float currentTrustLevel = confidenceLevel*this.confidenceToTrustLevelNormalizationFactor;
		if (trustLevelThreshold==null || currentTrustLevel>=trustLevelThreshold) {
			return buildInferenceForTrustLevel(this.decimalFormat.format(currentTrustLevel));
		} else {
			throw new TrustLevelThresholdExceededException();
		}
	}

	/**
	 * Returns inference object.
	 * @param trustLevel
	 * @return inference object
	 */
	private DataInfo buildInferenceForTrustLevel(String trustLevel) {
		DataInfo.Builder builder = DataInfo.newBuilder();
		builder.setInferred(true);
		builder.setTrust(trustLevel);
		Qualifier.Builder provenanceBuilder = Qualifier.newBuilder();
		provenanceBuilder.setClassid(HBaseConstants.SEMANTIC_CLASS_IIS);
		provenanceBuilder.setClassname(HBaseConstants.SEMANTIC_CLASS_IIS);
		provenanceBuilder.setSchemeid(HBaseConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS);
		provenanceBuilder.setSchemename(HBaseConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS);
		builder.setProvenanceaction(provenanceBuilder.build());
		builder.setInferenceprovenance(this.inferenceProvenance);
		return builder.build();
	}
	
	/**
	 * Builds relation metadata.
	 * @param schemaId
	 * @param classId
	 * @return relation metadata.
	 */
	protected RelMetadata buildRelMetadata(String schemaId, String classId) {
		RelMetadata.Builder relBuilder = RelMetadata.newBuilder();
		Qualifier.Builder qBuilder = Qualifier.newBuilder();
		qBuilder.setSchemeid(schemaId);
		qBuilder.setSchemename(schemaId);
		qBuilder.setClassid(classId);
		qBuilder.setClassname(classId);
		relBuilder.setSemantics(qBuilder.build());
		return relBuilder.build();
	}

	/**
	 * Clones builder provided as parameter, inverts relations and builds new Oaf object.
	 * Relation direction is not iverted as it is bidirectional.
	 * @param existingBuilder
	 * @return Oaf object containing relation with inverted source and target fields.
	 */
	protected Oaf invertBidirectionalRelationAndBuild(Oaf.Builder existingBuilder) {
//		works on builder clone to prevent changes in existing builder
		if (existingBuilder.getRel()!=null) {
			if (existingBuilder.getRel().getSource()!=null &&
					existingBuilder.getRel().getTarget()!=null) {
				Oaf.Builder builder = existingBuilder.clone();
				OafRel.Builder relBuilder = builder.getRelBuilder();
				String source = relBuilder.getSource();
				String target = relBuilder.getTarget();
				relBuilder.setSource(target);
				relBuilder.setTarget(source);
				builder.setRel(relBuilder.build());
				builder.setLastupdatetimestamp(System.currentTimeMillis());
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
	
	public String getPredefinedTrust() {
		return predefinedTrust;
	}
	
	/**
	 * Sets confidence to trust level normalization factor.
	 * @param confidenceToTrustLevelNormalizationFactor
	 */
	public void setConfidenceToTrustLevelNormalizationFactor(
			float confidenceToTrustLevelNormalizationFactor) {
		this.confidenceToTrustLevelNormalizationFactor = confidenceToTrustLevelNormalizationFactor;
	}

	/**
	 * Returns confidence to trust level normalization factor.
	 * @return confidence to trust level normalization factor
	 */
	public float getConfidenceToTrustLevelNormalizationFactor() {
		return confidenceToTrustLevelNormalizationFactor;
	}

}
