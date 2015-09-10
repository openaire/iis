package eu.dnetlib.iis.export.actionmanager.module;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.documentsclassification.schemas.DocumentToDocumentClasses;

/**
 * {@link DocumentToDocumentClasses} based action builder module.
 * @author mhorst
 *
 */
public class DocumentToDocumentClassesActionBuilderModuleFactory 
	implements ActionBuilderFactory<DocumentToDocumentClasses> {

	private static final AlgorithmName algorithmName = AlgorithmName.document_classes;
	
	class DocumentToDocumentClassesActionBuilderModule extends
	AbstractBuilderModule implements ActionBuilderModule<DocumentToDocumentClasses> {

		
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 */
		public DocumentToDocumentClassesActionBuilderModule(
				String predefinedTrust, Float trustLevelThreshold) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
		}
	
		@Override
		public List<AtomicAction> build(DocumentToDocumentClasses object,
				Agent agent, String actionSetId) {
			Oaf oaf = buildOAFClasses(object);
			if (oaf!=null) {
				return actionFactory.createUpdateActions(
						actionSetId,
						agent, object.getDocumentId().toString(), Type.result, 
						oaf.toByteArray());	
			} else {
				return Collections.emptyList();
			}
		}
	
		/**
		 * Builds OAF object containing document classes.
		 * @param source
		 * @return OAF object containing document classes
		 */
		protected Oaf buildOAFClasses(DocumentToDocumentClasses source) {
			if (source.getClasses()!=null) {
				Iterable<? extends StructuredProperty> classificationSubjects = convertAvroToProtoBuff(
						source.getClasses());
				if (classificationSubjects!=null) {
					OafEntity.Builder entityBuilder = OafEntity.newBuilder();
					Result.Builder resultBuilder = Result.newBuilder();
					Metadata.Builder metaBuilder = Metadata.newBuilder();
					metaBuilder.addAllSubject(classificationSubjects);
					resultBuilder.setMetadata(metaBuilder.build());
					entityBuilder.setResult(resultBuilder.build());
					entityBuilder.setId(source.getDocumentId().toString());	
					entityBuilder.setType(Type.result);
					return buildOaf(entityBuilder.build());
				}
			}
//			fallback
			return null;
		}
		
		private Iterable<? extends StructuredProperty> convertAvroToProtoBuff(eu.dnetlib.iis.documentsclassification.schemas.DocumentClasses source) {
			if (source!=null) {
				List<StructuredProperty> list = new ArrayList<StructuredProperty>(); 
				if (source.getArXivClasses()!=null && !source.getArXivClasses().isEmpty()) {
					list.addAll(convertAvroToProtoBuff(
							source.getArXivClasses(),
							HBaseConstants.SEMANTIC_CLASS_TAXONOMIES_ARXIV));
				}
				if (source.getDDCClasses()!=null && !source.getDDCClasses().isEmpty()) {
					list.addAll(convertAvroToProtoBuff(
							source.getDDCClasses(),
							HBaseConstants.SEMANTIC_CLASS_TAXONOMIES_DDC));
				}
				if (source.getWoSClasses()!=null && !source.getWoSClasses().isEmpty()) {
					list.addAll(convertAvroToProtoBuff(
							source.getWoSClasses(),
							HBaseConstants.SEMANTIC_CLASS_TAXONOMIES_WOS));
				}
				if (source.getMeshEuroPMCClasses()!=null && !source.getMeshEuroPMCClasses().isEmpty()) {
					list.addAll(convertAvroToProtoBuff(
							source.getMeshEuroPMCClasses(),
							HBaseConstants.SEMANTIC_CLASS_TAXONOMIES_MESHEUROPMC));
				}
				if (source.getACMClasses()!=null && !source.getACMClasses().isEmpty()) {
					list.addAll(convertAvroToProtoBuff(
							source.getACMClasses(),
							HBaseConstants.SEMANTIC_CLASS_TAXONOMIES_ACM));
				}
				
				if (!list.isEmpty()) {
					return list;
				} else {
					return null;
				}
			} else {
				return null;
			}
		}
		
		private List<StructuredProperty> convertAvroToProtoBuff(
				List<eu.dnetlib.iis.documentsclassification.schemas.DocumentClass> source,
				String taxonomyName) {
			List<StructuredProperty> result = new ArrayList<StructuredProperty>();
			for (eu.dnetlib.iis.documentsclassification.schemas.DocumentClass current : source) {
				if (current!=null) {
					try {
						result.add(convertAvroToProtoBuff(current, taxonomyName));
					} catch (TrustLevelThresholdExceededException e) {
//						no need to log, we just do not attach result
					}	
				}
			}
			return result;
		}
	//	
		private StructuredProperty convertAvroToProtoBuff(
				eu.dnetlib.iis.documentsclassification.schemas.DocumentClass source,
				String taxonomyName) throws TrustLevelThresholdExceededException {
			if (source!=null && source.getClassLabels()!=null && source.getClassLabels().size()>0) {
				StructuredProperty.Builder builder = StructuredProperty.newBuilder();
				Qualifier.Builder qualifierBuilder = Qualifier.newBuilder();
				qualifierBuilder.setSchemeid(
						HBaseConstants.SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES);
				qualifierBuilder.setSchemename(
						HBaseConstants.SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES);
				qualifierBuilder.setClassid(taxonomyName);
				qualifierBuilder.setClassname(taxonomyName);
				builder.setQualifier(qualifierBuilder.build());
				builder.setValue(StringUtils.join(source.getClassLabels(), 
						HBaseConstants.CLASSIFICATION_HIERARCHY_SEPARATOR));
				Float confidenceLevel = source.getConfidenceLevel();
				if (confidenceLevel!=null) {
					builder.setDataInfo(buildInference(
							confidenceLevel<1?confidenceLevel:1));
				} else {
					builder.setDataInfo(buildInference());
				}
				return builder.build();
			} else {
				return null;
			}
		}
	}

	@Override
	public ActionBuilderModule<DocumentToDocumentClasses> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		return new DocumentToDocumentClassesActionBuilderModule(
				predefinedTrust, trustLevelThreshold);
	}
	
	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}
}
