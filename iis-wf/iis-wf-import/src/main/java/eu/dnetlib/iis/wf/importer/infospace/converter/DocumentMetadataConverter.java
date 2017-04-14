package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import eu.dnetlib.data.mapreduce.util.OafRelDecoder;
import eu.dnetlib.data.proto.FieldTypeProtos.KeyValue;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.PersonResultProtos.PersonResult.Authorship;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultProtos;
import eu.dnetlib.data.proto.ResultProtos.Result.Instance;
import eu.dnetlib.data.proto.ResultProtos.Result.Journal;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.PublicationType;
import eu.dnetlib.iis.wf.importer.OafHelper;
import eu.dnetlib.iis.wf.importer.infospace.QualifiedOafJsonRecord;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;
import eu.dnetlib.iis.wf.importer.infospace.approver.ResultApprover;

/**
 * {@link OafEntity} containing document details to {@link DocumentMetadata} converter.
 * 
 * @author mhorst
 *
 */
public class DocumentMetadataConverter implements OafEntityWithRelsToAvroConverter<DocumentMetadata> {

    protected static final Logger log = Logger.getLogger(DocumentMetadataConverter.class);

    private static final String NULL_STRING_VALUE = "null";

    private static final String LANG_CLASSID_UNDEFINED = "und";

    /**
     * Person-result relation column family.
     */
    private final String personResultColumnFamily;

    /**
     * Result approver to be used when validating person result relations.
     */
    private ResultApprover resultApprover;
    
    /**
     * Field approver to be used when validating inferred fields.
     */
    private FieldApprover fieldApprover;

    // ------------------------ CONSTRUCTORS --------------------------
    
    /**
     * 
     * @param resultApprover approves {@link OafRel} objects 
     * @param fieldApprover approves fields
     */
    public DocumentMetadataConverter(ResultApprover resultApprover, FieldApprover fieldApprover) {
        this.resultApprover = Preconditions.checkNotNull(resultApprover);
        this.fieldApprover = Preconditions.checkNotNull(fieldApprover);
        this.personResultColumnFamily = OafRelDecoder.getCFQ(RelType.personResult, SubRelType.authorship, Authorship.RelName.hasAuthor.toString());
    }

    // ------------------------ LOGIC --------------------------
    
    @Override
    public DocumentMetadata convert(OafEntity oafEntity, Map<String, List<QualifiedOafJsonRecord>> relations) throws IOException {
        Preconditions.checkNotNull(oafEntity);
        
        if (!oafEntity.hasResult()) {
            log.error("skipping: no result object for id " + oafEntity.getId());
            return null;
        }
        DocumentMetadata.Builder builder = DocumentMetadata.newBuilder();
        builder.setId(oafEntity.getId());
        
        ResultProtos.Result sourceResult = oafEntity.getResult();
        createBasicMetadata(sourceResult, builder);
        handleAdditionalIds(oafEntity, builder);
        handleDatasourceIds(oafEntity, builder);
        List<QualifiedOafJsonRecord> personRelations = relations.get(personResultColumnFamily);
        if (CollectionUtils.isNotEmpty(personRelations)) {
            handlePersons(personRelations, builder);    
        }
        return builder.build();
    }

    // ------------------------ PRIVATE --------------------------
    
    /**
     * Creates basic metadata object.
     * 
     * @param sourceResult
     * @param metaBuilder
     * @return basic metadata object
     */
    private DocumentMetadata.Builder createBasicMetadata(ResultProtos.Result sourceResult,
            DocumentMetadata.Builder metaBuilder) {
        
        handlePublicationType(sourceResult.getInstanceList(), metaBuilder);
        
        if (sourceResult.hasMetadata()) {
            handleTitle(sourceResult.getMetadata().getTitleList(), metaBuilder);
            handleDescription(sourceResult.getMetadata().getDescriptionList(), metaBuilder);
            handleLanguage(sourceResult.getMetadata().getLanguage(), metaBuilder);
            handlePublisher(sourceResult.getMetadata().getPublisher(), metaBuilder);
            handleJournal(sourceResult.getMetadata().getJournal(), metaBuilder);
            handleYear(sourceResult.getMetadata().getDateofacceptance(), metaBuilder);
            handleKeywords(sourceResult.getMetadata().getSubjectList(), metaBuilder);    
        }
        
        return metaBuilder;
    }

    private void handleTitle(List<StructuredProperty> titleList, DocumentMetadata.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(titleList)) {
            for (StructuredProperty titleProp : titleList) {
                if (HBaseConstants.SEMANTIC_CLASS_MAIN_TITLE.equals(titleProp.getQualifier().getClassid())
                        && fieldApprover.approve(titleProp.getDataInfo())) {
                    metaBuilder.setTitle(titleProp.getValue());
                }
            }
            if (!metaBuilder.hasTitle()) {
                // if no main title available, setting first applicable title from the list
                for (StructuredProperty titleProp : titleList) {
                    if (fieldApprover.approve(titleProp.getDataInfo())) {
                        metaBuilder.setTitle(titleProp.getValue());
                        break;
                    }
                }
            }
        }
    }
    
    private void handleDescription(List<StringField> descriptionList, DocumentMetadata.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(descriptionList)) {
            for (StringField currentDescription : descriptionList) {
                if (fieldApprover.approve(currentDescription.getDataInfo()) 
                        && StringUtils.isNotBlank(currentDescription.getValue())
                        && !NULL_STRING_VALUE.equals(currentDescription.getValue())) {
                    metaBuilder.setAbstract$(currentDescription.getValue());
                    break;
                }
            }
        }
    }
    
    private void handleLanguage(Qualifier language, DocumentMetadata.Builder metaBuilder) {
        if (StringUtils.isNotBlank(language.getClassid())
                && !LANG_CLASSID_UNDEFINED.equals(language.getClassid())) {
            metaBuilder.setLanguage(language.getClassid());
        }    
    }
    
    private void handlePublisher(StringField publisher, DocumentMetadata.Builder metaBuilder) {
        if (StringUtils.isNotBlank(publisher.getValue())
                && fieldApprover.approve(publisher.getDataInfo())) {
            metaBuilder.setPublisher(publisher.getValue());
        }    
    }
    
    private void handleJournal(Journal journal, DocumentMetadata.Builder metaBuilder) {
        if (StringUtils.isNotBlank(journal.getName())
                && fieldApprover.approve(journal.getDataInfo())) {
            metaBuilder.setJournal(journal.getName());
        }    
    }
    
    private void handleYear(StringField dateOfAcceptance, DocumentMetadata.Builder metaBuilder) {
        if (fieldApprover.approve(dateOfAcceptance.getDataInfo())) {
            Integer yearValue = extractYear(dateOfAcceptance.getValue());
            if (yearValue != null) {
                metaBuilder.setYear(yearValue);
            }
        }    
    }
    private void handleKeywords(List<StructuredProperty> subjectList, DocumentMetadata.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(subjectList)) {
            // setting only selected subjects as keywords, skipping inferred data
            List<String> extractedKeywords = extractValues(subjectList);
            if (CollectionUtils.isNotEmpty(extractedKeywords)) {
                if (metaBuilder.getKeywords() == null) {
                    metaBuilder.setKeywords(new ArrayList<CharSequence>());
                }
                metaBuilder.getKeywords().addAll(extractedKeywords);
            }
        }    
    }
    
    private void handlePublicationType(List<Instance> instanceList, DocumentMetadata.Builder metaBuilder) {
        PublicationType.Builder publicationTypeBuilder = PublicationType.newBuilder();
        if (CollectionUtils.isNotEmpty(instanceList)) {
            for (Instance instance : instanceList) {
                if (HBaseConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE
                        .equals(instance.getInstancetype().getClassid())) {
                    publicationTypeBuilder.setArticle(true);
                } else if (HBaseConstants.SEMANTIC_CLASS_INSTANCE_TYPE_DATASET
                        .equals(instance.getInstancetype().getClassid())) {
                    publicationTypeBuilder.setDataset(true);
                }
            }
        }
        metaBuilder.setPublicationType(publicationTypeBuilder.build());
    }
    
    /**
     * Extracts year integer value from date.
     * 
     * @param date
     * @return year int value or null when provided date in invalid format
     */
    private static Integer extractYear(String date) {
        // expected date format: yyyy-MM-dd
        if (StringUtils.isNotBlank(date) && date.indexOf('-') == 4) {
            return Integer.valueOf(date.substring(0, date.indexOf('-')));
        } else {
            return null;
        }
    }

    /**
     * Handles additional identifiers.
     * 
     */
    private DocumentMetadata.Builder handleAdditionalIds(OafEntity oafEntity, DocumentMetadata.Builder metaBuilder) {
        // setting additional identifiers
        Map<CharSequence, CharSequence> additionalIds = new HashMap<CharSequence, CharSequence>();
        if (CollectionUtils.isNotEmpty(oafEntity.getPidList())) {
            for (StructuredProperty currentPid : oafEntity.getPidList()) {
                if (StringUtils.isNotBlank(currentPid.getQualifier().getClassid()) 
                        && StringUtils.isNotBlank(currentPid.getValue()) 
                        && fieldApprover.approve(currentPid.getDataInfo())) {
                    additionalIds.put(currentPid.getQualifier().getClassid(), currentPid.getValue());
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
     * 
     */
    private DocumentMetadata.Builder handleDatasourceIds(OafEntity oafEntity, DocumentMetadata.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(oafEntity.getCollectedfromList())) {
            List<CharSequence> datasourceIds = new ArrayList<CharSequence>(
                    oafEntity.getCollectedfromList().size());
            for (KeyValue currentCollectedFrom : oafEntity.getCollectedfromList()) {
                datasourceIds.add(currentCollectedFrom.getKey());
            }
            metaBuilder.setDatasourceIds(datasourceIds);
        }
        return metaBuilder;
    }

    /**
     * Handles persons.
     * 
     * @param relations person result relations
     * @param builder
     * @return builder with persons set
     * @throws IOException
     */
    private DocumentMetadata.Builder handlePersons(List<QualifiedOafJsonRecord> relations, DocumentMetadata.Builder builder)
            throws IOException {
        TreeMap<CharSequence, CharSequence> authorsSortedMap = new TreeMap<CharSequence, CharSequence>();
        for (QualifiedOafJsonRecord personResultRecord : relations) {
            Oaf persResOAF = OafHelper.buildOaf(personResultRecord.getOafJson());
            OafRel personResRel = persResOAF.getRel();
            if (resultApprover.approve(persResOAF)) {
                authorsSortedMap.put(personResRel.getPersonResult().getAuthorship().getRanking(),
                        personResRel.getTarget());
            }
        }
        // storing authors sorted by ranking
        if (!authorsSortedMap.isEmpty()) {
            builder.setAuthorIds(new ArrayList<CharSequence>(authorsSortedMap.values()));
        }
        return builder;
    }

    /**
     * Extracts values from {@link StructuredProperty} list. Checks DataInfo
     * element whether this piece of information should be approved.
     * 
     */
    private List<String> extractValues(Collection<StructuredProperty> source) {
        if (CollectionUtils.isNotEmpty(source)) {
            List<String> results = new ArrayList<String>(source.size());
            for (StructuredProperty current : source) {
                if (fieldApprover.approve(current.getDataInfo())) {
                    results.add(current.getValue());
                }
            }
            return results;
        } else {
            return Collections.emptyList();
        }
    }

}
