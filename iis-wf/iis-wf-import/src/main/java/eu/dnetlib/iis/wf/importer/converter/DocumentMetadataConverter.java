package eu.dnetlib.iis.wf.importer.converter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.data.proto.FieldTypeProtos.KeyValue;
import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.ResultProtos;
import eu.dnetlib.data.proto.ResultProtos.Result.Instance;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.PublicationType;
import eu.dnetlib.iis.wf.importer.OafHelper;
import eu.dnetlib.iis.wf.importer.input.approver.FieldApprover;
import eu.dnetlib.iis.wf.importer.input.approver.ResultApprover;

/**
 * HBase {@link Result} to avro {@link DocumentMetadata} converter.
 * @author mhorst
 *
 */public class DocumentMetadataConverter extends AbstractEncodingAwareAvroConverter<DocumentMetadata>{
	
	protected static final Logger log = Logger.getLogger(DocumentMetadataConverter.class);
	
	private static final String NULL_STRING_VALUE = "null";
	
	private static final String LANG_CLASSID_UNDEFINED = "und";
	
	/**
	 * Person-result relation column family.
	 */
	private final byte[] personResultAuthorshipHasAuthorColumnFamilyBytes;
	
	/**
	 * Field approver.
	 */
	protected final FieldApprover fieldApprover;
	
	/**
	 * Default constructor.
	 * @param encoding
	 * @param resultApprover
	 * @param fieldApprover
	 * @param personResultAuthorshipHasAuthorColumnFamilyBytes
	 */
	public DocumentMetadataConverter(String encoding, 
			ResultApprover resultApprover, FieldApprover fieldApprover, 
			byte[] personResultAuthorshipHasAuthorColumnFamilyBytes) {
		super(encoding, resultApprover);
		this.fieldApprover = fieldApprover;
		this.personResultAuthorshipHasAuthorColumnFamilyBytes = OafHelper.copyArrayWhenNotNull(
				personResultAuthorshipHasAuthorColumnFamilyBytes);
	}

	@Override
	public DocumentMetadata buildObject(Result hbaseResult, 
			Oaf resolvedOafObject) throws IOException {
		ResultProtos.Result sourceResult = resolvedOafObject.getEntity()!=null?
				resolvedOafObject.getEntity().getResult():null;
		if (sourceResult==null) {
			log.error("skipping: no result object " +
					"for a row " + new String(hbaseResult.getRow(), getEncoding()));
			return null;
		}
		if (resolvedOafObject.getEntity().getId()!=null) {
			DocumentMetadata.Builder builder = DocumentMetadata.newBuilder();
			builder.setId(resolvedOafObject.getEntity().getId());
			createBasicMetadata(sourceResult, builder);
			handleAdditionalIds(resolvedOafObject, builder);
			handleDatasourceIds(resolvedOafObject, builder);
			handlePersons(hbaseResult, builder);
			return builder.build();
		} else {
			log.error("skipping: no id specified for " +
					"result of a row " + new String(hbaseResult.getRow(), getEncoding()));
			return null;
		}
	}

	/**
	 * Creates basic metadata object.
	 * @param sourceResult
	 * @param metaBuilder
	 * @return basic metadata object
	 * @throws InvalidProtocolBufferException
	 */
	private DocumentMetadata.Builder createBasicMetadata(
			ResultProtos.Result sourceResult, DocumentMetadata.Builder metaBuilder) throws InvalidProtocolBufferException {
		if (sourceResult.getMetadata()!=null) {
			if (sourceResult.getMetadata().getTitleList()!=null) {
				if (sourceResult.getMetadata().getTitleList().size()>0) {
					for (StructuredProperty titleProp : sourceResult.getMetadata().getTitleList()) {
						if (titleProp.getQualifier()!=null && 
								HBaseConstants.SEMANTIC_CLASS_MAIN_TITLE.equals(titleProp.getQualifier().getClassid()) &&
								fieldApprover.approve(titleProp.getDataInfo())) {
							metaBuilder.setTitle(titleProp.getValue());
						}
					}
					if (!metaBuilder.hasTitle()) {
//						if no main title available, setting first applicable title from the list
						for (StructuredProperty titleProp : sourceResult.getMetadata().getTitleList()) {
							if (fieldApprover.approve(titleProp.getDataInfo())) {
								metaBuilder.setTitle(titleProp.getValue());		
							}
						}
					}
				}
			}
			if (sourceResult.getMetadata().getDescriptionList()!=null && 
					sourceResult.getMetadata().getDescriptionList().size()>0) {
				for (StringField currentDescription : sourceResult.getMetadata().getDescriptionList()) {
					if (fieldApprover.approve(currentDescription.getDataInfo()) && 
							currentDescription.getValue()!=null &&
							!NULL_STRING_VALUE.equals(currentDescription.getValue())) {
						metaBuilder.setAbstract$(currentDescription.getValue());
						break;
					}
				}
			}
			if (sourceResult.getMetadata().getLanguage()!=null &&
					sourceResult.getMetadata().getLanguage().getClassid()!=null && 
							!sourceResult.getMetadata().getLanguage().getClassid().isEmpty() &&
								!LANG_CLASSID_UNDEFINED.equals(
										sourceResult.getMetadata().getLanguage().getClassid())) {
//				language does not have dataInfo field, so we do not apply fieldApprover here
				metaBuilder.setLanguage(sourceResult.getMetadata().getLanguage().getClassid());
			}
			if (sourceResult.getMetadata().getPublisher()!=null && 
					sourceResult.getMetadata().getPublisher().getValue()!=null &&
					!sourceResult.getMetadata().getPublisher().getValue().isEmpty() &&
					fieldApprover.approve(sourceResult.getMetadata().getPublisher().getDataInfo())) {
				metaBuilder.setPublisher(sourceResult.getMetadata().getPublisher().getValue());
			}
			if (sourceResult.getMetadata().getJournal()!=null &&
					sourceResult.getMetadata().getJournal().getName()!=null &&
					!sourceResult.getMetadata().getJournal().getName().isEmpty() &&
					fieldApprover.approve(sourceResult.getMetadata().getJournal().getDataInfo())) {
				metaBuilder.setJournal(sourceResult.getMetadata().getJournal().getName());
			}
			
			if (sourceResult.getMetadata().getDateofacceptance()!=null && 
					sourceResult.getMetadata().getDateofacceptance().getValue()!=null &&
					fieldApprover.approve(sourceResult.getMetadata().getDateofacceptance().getDataInfo())) {
				Integer yearValue = extractYear(
						sourceResult.getMetadata().getDateofacceptance().getValue());
				if (yearValue!=null) {
					metaBuilder.setYear(yearValue);	
				}
			}
			if (sourceResult.getMetadata().getSubjectList()!=null && 
					sourceResult.getMetadata().getSubjectList().size()>0) {
//				setting only selected subjects as keywords, skipping inferred data
				Collection<String> extractedKeywords = extractValues(
						sourceResult.getMetadata().getSubjectList());
				if (extractedKeywords!=null && extractedKeywords.size()>0) {
					if (metaBuilder.getKeywords()==null) {
						metaBuilder.setKeywords(new ArrayList<CharSequence>());
					}
					metaBuilder.getKeywords().addAll(extractedKeywords);
				}
			}
//			setting publication type
			PublicationType.Builder publicationTypeBuilder = PublicationType.newBuilder();
			if (sourceResult.getInstanceCount()>0) {
				for (Instance instance : sourceResult.getInstanceList()) {
					if (instance.getInstancetype()!=null && instance.getInstancetype().getClassid()!=null) {
						if (HBaseConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE.equals(
								instance.getInstancetype().getClassid())) {
							publicationTypeBuilder.setArticle(true);
						} else if (HBaseConstants.SEMANTIC_CLASS_INSTANCE_TYPE_DATASET.equals(
								instance.getInstancetype().getClassid())) {
							publicationTypeBuilder.setDataset(true);
						}
					}
				}
			}
			metaBuilder.setPublicationType(publicationTypeBuilder.build());
		}
		return metaBuilder;
	}
	
	/**
	 * Extracts year integer value from date.
	 * @param date
	 * @return year int value or null when provided date in invalid format
	 */
	private static Integer extractYear(String date) {
//		expected date format: yyyy-MM-dd
		if (date!=null && date.length()>0 && date.indexOf('-')==4) {
			return Integer.valueOf(date.substring(0, date.indexOf('-')));
		} else {
			return null;	
		}
	}
	
	/**
	 * Handles additional identifiers.
	 * @param resolvedOafObject
	 * @param metaBuilder
	 * @return
	 * @throws InvalidProtocolBufferException
	 */
	private DocumentMetadata.Builder handleAdditionalIds(Oaf resolvedOafObject,
			DocumentMetadata.Builder metaBuilder) throws InvalidProtocolBufferException {
//		setting additional identifiers
		Map<CharSequence,CharSequence> additionalIds = new HashMap<CharSequence, CharSequence>();
		if (resolvedOafObject!=null && resolvedOafObject.getEntity()!=null &&
				resolvedOafObject.getEntity().getPidList()!=null) {
			for (StructuredProperty currentPid : resolvedOafObject.getEntity().getPidList()) {
//				TODO add schemeId control, limit processing for supported schemeids
				if (currentPid!=null && currentPid.getQualifier()!=null && 
						currentPid.getQualifier().getClassid()!=null &&
						currentPid.getValue()!=null &&
						fieldApprover.approve(currentPid.getDataInfo())) {
					additionalIds.put(
							currentPid.getQualifier().getClassid(), 
							currentPid.getValue());
				}
			}
		}
		if (!additionalIds.isEmpty()) {
			metaBuilder.setExternalIdentifiers(additionalIds);
		}
		return metaBuilder;
	}
	
	/**
	 * Handles datasource identifiers.
	 * @param resolvedOafObject
	 * @param metaBuilder
	 * @return
	 * @throws InvalidProtocolBufferException
	 */
	private DocumentMetadata.Builder handleDatasourceIds(Oaf resolvedOafObject,
			DocumentMetadata.Builder metaBuilder) throws InvalidProtocolBufferException {
		if (resolvedOafObject!=null && resolvedOafObject.getEntity()!=null) {
			if (resolvedOafObject.getEntity().getCollectedfromCount()>0) {
				List<CharSequence> datasourceIds = new ArrayList<CharSequence>(
						resolvedOafObject.getEntity().getCollectedfromCount());
				for (KeyValue currentCollectedFrom : resolvedOafObject.getEntity().getCollectedfromList()) {
					datasourceIds.add(currentCollectedFrom.getKey());
				}
				metaBuilder.setDatasourceIds(datasourceIds);
			}
		}
		return metaBuilder;
	}
	
	/**
	 * Handles persons.
	 * @param hbaseResult
	 * @param builder
	 * @return builder with persons handled
	 * @throws IOException
	 */
	private DocumentMetadata.Builder handlePersons(Result hbaseResult, 
			DocumentMetadata.Builder builder) throws IOException {
		NavigableMap<byte[],byte[]> personResultRelations = hbaseResult.getFamilyMap(
				personResultAuthorshipHasAuthorColumnFamilyBytes);
		TreeMap<CharSequence, CharSequence> authorsSortedMap = new TreeMap<CharSequence, CharSequence>();
		for (byte[] personResultBytes : personResultRelations.values()) {
			Oaf persResOAF = OafHelper.buildOaf(personResultBytes);
			OafRel personResRel = persResOAF.getRel();
			if (resultApprover!=null?
					resultApprover.approveBeforeBuilding(persResOAF):
						true) {
				authorsSortedMap.put(
						personResRel.getPersonResult().getAuthorship().getRanking(), 
						personResRel.getTarget());
			}
		}
//		storing authors sorted by ranking
		if (!authorsSortedMap.isEmpty()) {
			builder.setAuthorIds(new ArrayList<CharSequence>(authorsSortedMap.values()));	
		}
		return builder;
	}
	
	/**
	 * Extracts values from {@link StructuredProperty} list.
	 * Checks DataInfo element whether this piece of information was not generated by IIS.
	 * @param source
	 * @return extracted values.
	 */
	private Collection<String> extractValues(Collection<StructuredProperty> source) {
		if (source!=null) {
			List<String> results = new ArrayList<String>(source.size());
			for(StructuredProperty current : source) {
				if (fieldApprover.approve(current.getDataInfo())) {
					results.add(current.getValue());	
				}
			}
			return results;
		} else {
			return null;
		}
	}
	
}
