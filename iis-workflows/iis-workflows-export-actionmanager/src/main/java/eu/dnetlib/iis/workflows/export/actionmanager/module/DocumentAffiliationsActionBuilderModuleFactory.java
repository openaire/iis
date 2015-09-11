package eu.dnetlib.iis.workflows.export.actionmanager.module;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.data.mapreduce.util.OafDecoder;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.OrganizationProtos.Organization;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.ResultOrganizationProtos.ResultOrganization;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.data.transform.xml.AbstractDNetOafXsltFunctions;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.export.schemas.DocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.Affiliation;

/**
 * {@link DocumentMetadata} based action builder module.
 * @author mhorst
 *
 */
public class DocumentAffiliationsActionBuilderModuleFactory 
	implements ActionBuilderFactory<DocumentMetadata> {
	
	private static final String COUNTRIES_VOCABULARY_LOCATION = "countries.vocabulary.location";

	private static final AlgorithmName algorithmName = AlgorithmName.document_affiliations;
	
	private static final String IIS_NS_PREFIX = "iisinference";
	
	private static final Logger log = Logger.getLogger(DocumentAffiliationsActionBuilderModuleFactory.class);
	
	class DocumentMetadataActionBuilderModule extends AbstractBuilderModule
	implements ActionBuilderModule<DocumentMetadata> {
	
		private final String entityTargetColumn;
		
		private final Properties countriesVocabulary;
		
		/**
		 * Default constructor.
		 * @param predefinedTrust
		 * @param trustLevelThreshold
		 * @param countriesVocabulary
		 */
		public DocumentMetadataActionBuilderModule(String predefinedTrust,
				Float trustLevelThreshold,
				Properties countriesVocabulary) {
			super(predefinedTrust, trustLevelThreshold, algorithmName);
			this.countriesVocabulary = countriesVocabulary;
			try {
				entityTargetColumn = new String(
						HBaseConstants.QUALIFIER_BODY,
						HBaseConstants.STATIC_FIELDS_ENCODING_UTF8);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
	
		@Override
		public List<AtomicAction> build(DocumentMetadata object, Agent agent,
				String actionSetId) {
			
			System.out.println("got affiliations for id: " + object.getId().toString() + 
					": " + buildAffiliationsString(object.getAffiliations()));
//			TODO enable affiliations export
//			if (countriesVocabulary==null) {
//			throw new RuntimeException("countries vocabulary not provided, "
//					+ "missing property value: " + COUNTRIES_VOCABULARY_LOCATION);
//			}
//			return buildAffiliationActions(object, agent, actionSetId);
			return null;
		}
		
		/**
		 * Builds affiliation actions containing both result to organization relations and organization entities.
		 * @param object
		 * @param agent
		 * @param actionSetId
		 * @return list of actions containing both result to organization relations and organization entities
		 */
		protected List<AtomicAction> buildAffiliationActions(DocumentMetadata object, Agent agent,
				String actionSetId) {
			String docId = object.getId().toString();
			if (object.getAffiliations()!=null && !object.getAffiliations().isEmpty()) {
				List<AtomicAction> actions = new ArrayList<AtomicAction>();
				for (Affiliation currentAffiliation : object.getAffiliations()) {
					String organizationName = currentAffiliation.getOrganization()!=null?
							currentAffiliation.getOrganization().toString():null;
					if (organizationName==null || organizationName.isEmpty()) {
						log.warn("skipping organization entity and relations export: "
								+ "empty organization name for document: " + object.getId() + 
								", raw affiliation text: " + currentAffiliation.getRawText());
						continue;
					}
					String currentOrganizationId = buildOrganizationIndetifier(
							currentAffiliation.getRawText().toString());
//					adding organization entity
					Oaf organizationEntity = buildOrganizationOaf(
							currentOrganizationId, 
							organizationName, 
							currentAffiliation.getCountryCode()!=null?
									currentAffiliation.getCountryCode().toString():null);
					actions.add(actionFactory.createAtomicAction(actionSetId,
							agent, currentOrganizationId, 
							OafDecoder.decode(organizationEntity).getCFQ(), 
							entityTargetColumn, organizationEntity.toByteArray()));
//					adding result-organization bidirectional relations
					Oaf.Builder oafBuilder = Oaf.newBuilder();
					oafBuilder.setKind(Kind.relation);
					OafRel.Builder relBuilder = OafRel.newBuilder();
					relBuilder.setChild(false);
					relBuilder.setRelType(RelType.resultOrganization);
//					FIXME subreltype and relclass are currently not defined for RelType.resultOrganization
//					relBuilder.setSubRelType(SubRelType.???);
//					relBuilder.setRelClass(REL_CLASS_SOME_REL_CLASS);
					relBuilder.setSource(docId);
					relBuilder.setTarget(currentOrganizationId);
					ResultOrganization.Builder resOrganizationBuilder = ResultOrganization.newBuilder();
//					FIXME currently no RelMetadata fields to set
//					resOrganizationBuilder.setRelMetadata(buildRelMetadata(
//						HBaseConstants.SEMANTIC_SCHEME_DNET_RELATIONS_RESULT_ORGANIZATION,
//						REL_CLASS_SOME_REL_CLASS));
					relBuilder.setResultOrganization(resOrganizationBuilder.build());
					oafBuilder.setRel(relBuilder.build());
					oafBuilder.setDataInfo(buildInference());
					oafBuilder.setTimestamp(System.currentTimeMillis());
					Oaf oaf = oafBuilder.build();
					Oaf oafInverted = invertBidirectionalRelationAndBuild(oafBuilder);
					actions.add(actionFactory.createAtomicAction(actionSetId,
							agent, docId, OafDecoder.decode(oaf).getCFQ(),
							currentOrganizationId, oaf.toByteArray()));
//					setting reverse relation in referenced object
					actions.add(actionFactory.createAtomicAction(actionSetId,
							agent, currentOrganizationId, OafDecoder.decode(oafInverted).getCFQ(), 
							docId, oafInverted.toByteArray()));
				}
				return actions;
			} else {
				return null;
			}
		}
		
		String buildAffiliationsString(List<Affiliation> affiliations) {
			if (affiliations!=null) {
				StringBuilder strBuilder = new StringBuilder();
				for (Affiliation affiliation : affiliations) {
					strBuilder.append("{");
					strBuilder.append("\"organization\": ");
					strBuilder.append('"');
					strBuilder.append(affiliation.getOrganization());
					strBuilder.append('"');
					strBuilder.append(',');
					strBuilder.append("\"countryName\": ");
					strBuilder.append('"');
					strBuilder.append(affiliation.getCountryName());
					strBuilder.append('"');
					strBuilder.append(',');
					strBuilder.append("\"countryCode\": ");
					strBuilder.append('"');
					strBuilder.append(affiliation.getCountryCode());
					strBuilder.append('"');
					strBuilder.append(',');
					strBuilder.append("\"rawText\": ");
					strBuilder.append('"');
					strBuilder.append(affiliation.getRawText());
					strBuilder.append('"');
					strBuilder.append("}");
				}
				return strBuilder.toString();
			} else {
				return "null";
			}
		}
		
		
		/**
		 * Builds OA+ identifier for given orgranization name.
		 * @param organizationText cannot be null
		 * @return OA+ organization identifier
		 */
		String buildOrganizationIndetifier(String organizationText) {
			return AbstractDNetOafXsltFunctions.oafId(
	        		Type.organization.name(), IIS_NS_PREFIX, organizationText);
		}
		
		/**
		 * Builds inferred organization entity.
		 * @param id cannot be null
		 * @param name cannot be null
		 * @param countryCode
		 * @return inferred organization entity
		 */
		Oaf buildOrganizationOaf(String id, String name, String countryCode) {
			OafEntity.Builder orgEntityBuilder = OafEntity.newBuilder();
			Organization.Builder orgBuilder = Organization.newBuilder();
			Organization.Metadata.Builder orgMetaBuilder = Organization.Metadata.newBuilder();
			StringField.Builder nameBuilder = StringField.newBuilder();
			nameBuilder.setValue(name.toString());
			orgMetaBuilder.setLegalname(nameBuilder.build());
			if (countryCode!=null) {
				Qualifier.Builder countryBuilder = Qualifier.newBuilder();
//				TODO set proper scheme
				countryBuilder.setSchemeid("dnet:countries");
				countryBuilder.setSchemename("dnet:countries");
				countryBuilder.setClassid(countryCode);
				String countryName = countriesVocabulary.getProperty(countryCode);
				if (countryName!=null) {
					countryBuilder.setClassname(countryName);	
				} else {
					log.warn("got null country name from vocabulary for country code: " + countryCode);
				}
				orgMetaBuilder.setCountry(countryBuilder.build());
			}
			orgBuilder.setMetadata(orgMetaBuilder.build());
			orgEntityBuilder.setOrganization(orgBuilder.build());
			orgEntityBuilder.setId(id);
			orgEntityBuilder.setType(Type.organization);
			Oaf organizationOaf = buildOaf(
					orgEntityBuilder.build(), buildInference());
			return organizationOaf;
		}
	}

	@Override
	public ActionBuilderModule<DocumentMetadata> instantiate(
			String predefinedTrust, Float trustLevelThreshold, Configuration config) {
		String countriesVocabularyLocation = config.get(COUNTRIES_VOCABULARY_LOCATION);
		Properties countriesVocabularyProps = null;
		if (countriesVocabularyLocation!=null) {
			try {
				FileSystem fs = FileSystem.get(config);
				countriesVocabularyProps = new Properties();
				countriesVocabularyProps.load(fs.open(new Path(countriesVocabularyLocation)));	
			} catch (IOException e) {
				throw new RuntimeException("exception occurred while loading vocabularies", e);
			}
		} 
		return new DocumentMetadataActionBuilderModule(
				predefinedTrust, trustLevelThreshold, countriesVocabularyProps);	
		
	}
	
	@Override
	public AlgorithmName getAlgorithName() {
		return algorithmName;
	}
}
