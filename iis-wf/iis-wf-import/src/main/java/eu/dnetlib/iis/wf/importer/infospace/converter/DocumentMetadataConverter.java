package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.io.IOException;
import java.time.Year;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.importer.schemas.Author;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.PublicationType;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;

/**
 * {@link Result} containing document details to {@link DocumentMetadata} converter.
 * 
 * @author mhorst
 *
 */
public class DocumentMetadataConverter implements OafEntityToAvroConverter<Result, DocumentMetadata> {

    protected static final Logger log = Logger.getLogger(DocumentMetadataConverter.class);

    private static final String NULL_STRING_VALUE = "null";

    private static final String LANG_CLASSID_UNDEFINED = "und";

    /**
     * Field approver to be used when validating inferred fields.
     */
    private FieldApprover fieldApprover;

    // ------------------------ CONSTRUCTORS --------------------------
    
    /**
     * 
     * @param fieldApprover approves fields
     */
    public DocumentMetadataConverter(FieldApprover fieldApprover) {
        this.fieldApprover = Preconditions.checkNotNull(fieldApprover);
    }

    // ------------------------ LOGIC --------------------------
    
    @Override
    public DocumentMetadata convert(Result result) throws IOException {
        Preconditions.checkNotNull(result);
        
        DocumentMetadata.Builder builder = DocumentMetadata.newBuilder();
        builder.setId(result.getId());

        createBasicMetadata(result, builder);
        handleAdditionalIds(result, builder);
        handleDatasourceIds(result, builder);
        handlePersons(result, builder);
        
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
    private DocumentMetadata.Builder createBasicMetadata(Result sourceResult,
            DocumentMetadata.Builder metaBuilder) {
        
        handlePublicationType(sourceResult.getInstance(), metaBuilder);
        handleTitle(sourceResult.getTitle(), metaBuilder);
        handleDescription(sourceResult.getDescription(), metaBuilder);
        handleLanguage(sourceResult.getLanguage(), metaBuilder);
        handlePublisher(sourceResult.getPublisher(), metaBuilder);
        if (sourceResult instanceof Publication) {
            handleJournal(((Publication)sourceResult).getJournal(), metaBuilder);
        }
        handleYear(sourceResult.getDateofacceptance(), metaBuilder);
        handleKeywords(sourceResult.getSubject(), metaBuilder);    
        
        return metaBuilder;
    }

    private void handleTitle(List<StructuredProperty> titleList, DocumentMetadata.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(titleList)) {
            for (StructuredProperty titleProp : titleList) {
                if (titleProp.getQualifier() != null && InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE.equals(titleProp.getQualifier().getClassid())
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
    
    private void handleDescription(List<Field<String>> descriptionList, DocumentMetadata.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(descriptionList)) {
            for (Field<String> currentDescription : descriptionList) {
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
        if (language != null && StringUtils.isNotBlank(language.getClassid())
                && !LANG_CLASSID_UNDEFINED.equals(language.getClassid())) {
            metaBuilder.setLanguage(language.getClassid());
        }    
    }
    
    private void handlePublisher(Field<String> publisher, DocumentMetadata.Builder metaBuilder) {
        if (publisher != null && StringUtils.isNotBlank(publisher.getValue())
                && fieldApprover.approve(publisher.getDataInfo())) {
            metaBuilder.setPublisher(publisher.getValue());
        }    
    }
    
    private void handleJournal(Journal journal, DocumentMetadata.Builder metaBuilder) {
        if (journal != null && StringUtils.isNotBlank(journal.getName())
                && fieldApprover.approve(journal.getDataInfo())) {
            metaBuilder.setJournal(journal.getName());
        }    
    }
    
    private void handleYear(Field<String> dateOfAcceptance, DocumentMetadata.Builder metaBuilder) {
        if (dateOfAcceptance != null && fieldApprover.approve(dateOfAcceptance.getDataInfo())) {
            Year year = MetadataConverterUtils.extractYearOrNull(dateOfAcceptance.getValue(), log);
            if (year != null) {
                metaBuilder.setYear(year.getValue());
            }
        }    
    }
    private void handleKeywords(List<StructuredProperty> subjectList, DocumentMetadata.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(subjectList)) {
            // setting only selected subjects as keywords, skipping inferred data
            List<String> extractedKeywords = MetadataConverterUtils.extractValues(subjectList, fieldApprover);
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
                if (InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_ARTICLE
                        .equals(instance.getInstancetype().getClassid())) {
                    publicationTypeBuilder.setArticle(true);
                } else if (InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_DATASET
                        .equals(instance.getInstancetype().getClassid())) {
                    publicationTypeBuilder.setDataset(true);
                }
            }
        }
        metaBuilder.setPublicationType(publicationTypeBuilder.build());
    }


    /**
     * Handles additional identifiers.
     * 
     */
    private DocumentMetadata.Builder handleAdditionalIds(OafEntity oafEntity, DocumentMetadata.Builder metaBuilder) {
        // setting additional identifiers
        if (CollectionUtils.isNotEmpty(oafEntity.getPid())) {
            Map<CharSequence, CharSequence> additionalIds = new HashMap<CharSequence, CharSequence>();    
            for (StructuredProperty currentPid : oafEntity.getPid()) {
                if (StringUtils.isNotBlank(currentPid.getQualifier().getClassid()) 
                        && StringUtils.isNotBlank(currentPid.getValue()) 
                        && fieldApprover.approve(currentPid.getDataInfo())) {
                    additionalIds.put(currentPid.getQualifier().getClassid(), currentPid.getValue());
                }
            }
            if (MapUtils.isNotEmpty(additionalIds)) {
                metaBuilder.setExternalIdentifiers(additionalIds);
            }    
        }
        return metaBuilder;
    }

    /**
     * Handles datasource identifiers.
     * 
     */
    private DocumentMetadata.Builder handleDatasourceIds(OafEntity oafEntity, DocumentMetadata.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(oafEntity.getCollectedfrom())) {
            List<CharSequence> datasourceIds = new ArrayList<CharSequence>(
                    oafEntity.getCollectedfrom().size());
            for (KeyValue currentCollectedFrom : oafEntity.getCollectedfrom()) {
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
    private DocumentMetadata.Builder handlePersons(Result result, DocumentMetadata.Builder builder)
            throws IOException {
        if (CollectionUtils.isNotEmpty(result.getAuthor())) {
            List<Author> authors = new ArrayList<>();
            for (eu.dnetlib.dhp.schema.oaf.Author sourceAuthor : result.getAuthor()) {
                Author.Builder authorBuilder = Author.newBuilder();
                handleName(sourceAuthor.getName(), authorBuilder);
                handleSurname(sourceAuthor.getSurname(), authorBuilder);
                handleFullName(sourceAuthor.getFullname(), authorBuilder);
                if (isDataValid(authorBuilder)) {
                    authors.add(authorBuilder.build());
                }
            }
            if (!authors.isEmpty()) {
                builder.setAuthors(authors);
            }    
        }
        return builder;
    }

    private void handleName(String firstName, Author.Builder builder) {
        if (StringUtils.isNotBlank(firstName)) {
            builder.setName(firstName);
        }
    }
    
    private void handleSurname(String surname, Author.Builder builder) {
        if (StringUtils.isNotBlank(surname)) {
            builder.setSurname(surname);
        }
    }
    
    private void handleFullName(String fullName, Author.Builder builder) {
        if (StringUtils.isNotBlank(fullName)) {
            builder.setFullname(fullName);
        }
    }
    
    private boolean isDataValid(Author.Builder builder) {
        return builder.hasName() || builder.hasSurname() || builder.hasFullname();
    }


}
