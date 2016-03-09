package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos.Result;
import eu.dnetlib.data.proto.ResultProtos.Result.Journal;
import eu.dnetlib.data.proto.ResultProtos.Result.Metadata;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;

/**
 * {@link ExtractedDocumentMetadata} based action builder module.
 * @author mhorst
 *
 */
public class ExtractedDocumentMetadataActionBuilderModuleFactory implements ActionBuilderFactory<ExtractedDocumentMetadata> {

	private static final AlgorithmName algorithmName = AlgorithmName.document_affiliations;
	
	class ExtractedDocumentMetadataActionBuilderModule extends
	AbstractBuilderModule implements ActionBuilderModule<ExtractedDocumentMetadata> {
	
		/**
		 * Flag indicating overwriting already existing data should be prevented.
		 */
		private final boolean skipOriginalMetadata;
		
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 * @param trustLevelThreshold
		 * @param skipOriginalMetadata flag indicating overwriting already existing data should be prevented
		 */
		public ExtractedDocumentMetadataActionBuilderModule(String predefinedTrust,
				Float trustLevelThreshold, boolean skipOriginalMetadata) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
			this.skipOriginalMetadata = skipOriginalMetadata;
		}
	
		@Override
		public List<AtomicAction> build(ExtractedDocumentMetadata object,
				Agent agent, String actionSetId) {
			Oaf oaf = buildOAFMeta(object);
			if (oaf!=null) {
				return actionFactory.createUpdateActions(
						actionSetId,
						agent, object.getId().toString(), Type.result, 
						oaf.toByteArray());	
			} else {
				return Collections.emptyList();
			}
		}
	
		/**
		 * Builds OAF object.
		 * @param source
		 * @return OAF object
		 */
		protected Oaf buildOAFMeta(ExtractedDocumentMetadata source) {
			Result result = buildResultMeta(source);
			if (result!=null || shouldExternalIdentifiersBeSet(source)) {
				eu.dnetlib.data.proto.OafProtos.OafEntity.Builder entityBuilder = OafEntity.newBuilder();
				if (source.getId()!=null) {
					entityBuilder.setId(source.getId().toString());	
				}
				entityBuilder.setType(Type.result);
	//			setting result when present
				if (result!=null) {
					entityBuilder.setResult(result);	
				}
	//			setting alternative identifiers when present
				if (shouldExternalIdentifiersBeSet(source)) {
					for (Entry<CharSequence, CharSequence> typedId : source.getExternalIdentifiers().entrySet()) {
						entityBuilder.addPid(convertToStructuredProperty(typedId.getValue(), 
								HBaseConstants.SEMANTIC_SCHEME_DNET_PID_TYPES, 
								typedId.getKey().toString()));
					}
				}
				return buildOaf(entityBuilder.build());
			} else {
				return null;
			}
		}
	
		/**
		 * Builds document result.
		 * @param source
		 * @return document result
		 */
		private Result buildResultMeta(ExtractedDocumentMetadata source) {
			eu.dnetlib.data.proto.ResultProtos.Result.Builder resultBuilder = Result.newBuilder();
			if (!this.skipOriginalMetadata) {
				boolean wasBasicMetaModified = false;
				eu.dnetlib.data.proto.ResultProtos.Result.Metadata.Builder metaBuilder = Metadata.newBuilder();
	
				if (source.getTitle()!=null) {
					metaBuilder.addTitle(convertToStructuredProperty(source.getTitle(), 
							HBaseConstants.SEMANTIC_SCHEME_DNET_TITLE_TYPOLOGIES, 
							HBaseConstants.SEMANTIC_CLASS_MAIN_TITLE));
					wasBasicMetaModified = true;
				}
	
				if (source.getAbstract$()!=null && 
						isStringNotEmpty(source.getAbstract$().toString())) {
					StringField.Builder strFieldBuilder = StringField.newBuilder();
					strFieldBuilder.setValue(source.getAbstract$().toString());
					strFieldBuilder.setDataInfo(buildInference());
					metaBuilder.addDescription(strFieldBuilder.build());
					wasBasicMetaModified = true;
				}
	
				if (source.getPublisher()!=null && isStringNotEmpty(source.getPublisher().toString())) {
					StringField.Builder strFieldBuilder = StringField.newBuilder();
					strFieldBuilder.setValue(source.getPublisher().toString());
					strFieldBuilder.setDataInfo(buildInference());
					metaBuilder.setPublisher(strFieldBuilder.build());
					wasBasicMetaModified = true;
				}
				
				if (source.getJournal()!=null && isStringNotEmpty(source.getJournal().toString())) {
					Journal.Builder journalBuilder = Journal.newBuilder();
					journalBuilder.setName(source.getJournal().toString());
					metaBuilder.setJournal(journalBuilder.build());
					wasBasicMetaModified = true;
				}
				
				if (source.getLanguage()!=null &&
						isStringNotEmpty(source.getLanguage().toString())) {
					Qualifier.Builder qBuilder = Qualifier.newBuilder();
					qBuilder.setClassid(source.getLanguage().toString());
	//				TODO classname should should contain full language name ('English') rather than lang code ('eng')
					qBuilder.setClassname(source.getLanguage().toString());
					qBuilder.setSchemeid(HBaseConstants.SEMANTIC_SCHEME_DNET_LANGUAGES);
					qBuilder.setSchemename(HBaseConstants.SEMANTIC_SCHEME_DNET_LANGUAGES);
					metaBuilder.setLanguage(qBuilder.build());
					wasBasicMetaModified = true;
				}
	//			setting keywords as subjects
				if (source.getKeywords()!=null && source.getKeywords().size()>0) {
	//				TODO what is default subject classid and schemeid?
					metaBuilder.addAllSubject(convertToStructuredProperty(
							source.getKeywords(), null,null));
					wasBasicMetaModified = true;
				}
				if (wasBasicMetaModified) {
					resultBuilder.setMetadata(metaBuilder.build());
				} 
			}
			return resultBuilder.hasMetadata()?resultBuilder.build():null;
		}
	
		/**
		 * Converts to list containing structured properties
		 * @param source
		 * @param defaultScheme
		 * @param defaultClass
		 * @return structured properties list
		 */
		Collection<StructuredProperty> convertToStructuredProperty(Collection<CharSequence> source,
				String defaultScheme, String defaultClass) {
			if (source!=null) {
				List<StructuredProperty> results = new ArrayList<StructuredProperty>(
						source.size());
				for (CharSequence current : source) {
					results.add(convertToStructuredProperty(
							current, defaultScheme, defaultClass));
				}
				return results;
			} else {
				return null;
			}
		}
		
		/**
		 * Converts to structured property
		 * @param source
		 * @param defaultScheme
		 * @param defaultClass
		 * @return structured properties list
		 */
		StructuredProperty convertToStructuredProperty(CharSequence source,
				String defaultScheme, String defaultClass) {
			if (source!=null) {
				StructuredProperty.Builder builder = StructuredProperty.newBuilder();
				builder.setValue(source.toString());	
				if (defaultScheme!=null || defaultClass!=null) {
					Qualifier.Builder qBuilder = Qualifier.newBuilder();
					if (defaultClass!=null) {
						qBuilder.setClassid(defaultClass);
						qBuilder.setClassname(defaultClass);	
					}
					if (defaultScheme!=null) {
						qBuilder.setSchemeid(defaultScheme);
						qBuilder.setSchemename(defaultScheme);	
					}
					builder.setQualifier(qBuilder.build());
				}
				return builder.build();
			} else {
				return null;
			}
		}
		
		/**
		 * Checks whether string is not empty.
		 * @param str
		 * @return true when not empty, false otherwise
		 */
		private boolean isStringNotEmpty(String str) {
			return str!=null && str.length()>0;
		}
		
		/**
		 * Checks whether any external identifiers should be set.
		 * @param source
		 * @return true when any external identifiers should be set, false otherwise
		 */
		private boolean shouldExternalIdentifiersBeSet(ExtractedDocumentMetadata source) {
			return !skipOriginalMetadata && source.getExternalIdentifiers()!=null && 
					!source.getExternalIdentifiers().isEmpty();
		}

	}
	
	@Override
	public ActionBuilderModule<ExtractedDocumentMetadata> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		return new ExtractedDocumentMetadataActionBuilderModule(
				predefinedTrust, trustLevelThreshold, true);
	}

	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}
}
