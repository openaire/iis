package eu.dnetlib.iis.wf.importer.infospace.converter;

import com.google.common.base.Preconditions;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.importer.schemas.Author;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.PublicationType;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Year;
import java.util.*;
import java.util.stream.Collectors;

/**
 * {@link Result} containing document details to {@link DocumentMetadata} converter.
 * 
 * @author mhorst
 *
 */
public class DocumentMetadataConverter implements OafEntityToAvroConverter<Result, DocumentMetadata> {

    private static final long serialVersionUID = 467106069692603946L;

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
     */
    private void createBasicMetadata(Result sourceResult,
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

    }

    private void handleTitle(List<StructuredProperty> titleList, DocumentMetadata.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(titleList)) {

            titleList.stream().filter(x -> Objects.nonNull(x.getQualifier()))
                    .filter(x -> InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE.equals(x.getQualifier().getClassid()))
                    .filter(x -> StringUtils.isNotBlank(x.getValue()))
                    .filter(x -> fieldApprover.approve(x.getDataInfo())).findFirst()
                    .ifPresent(x -> metaBuilder.setTitle(x.getValue()));

            if (!metaBuilder.hasTitle()) {
                // if no main title available, setting first applicable title from the list
                titleList.stream().filter(x -> fieldApprover.approve(x.getDataInfo())).findFirst()
                        .ifPresent(x -> metaBuilder.setTitle(x.getValue()));
            }
        }
    }
    
    private void handleDescription(List<Field<String>> descriptionList, DocumentMetadata.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(descriptionList)) {
            descriptionList.stream().filter(x -> fieldApprover.approve(x.getDataInfo()))
                    .filter(x -> StringUtils.isNotBlank(x.getValue()))
                    .filter(x -> !NULL_STRING_VALUE.equals(x.getValue())).findFirst()
                    .ifPresent(x -> metaBuilder.setAbstract$(x.getValue()));
        }
    }
    
    private static void handleLanguage(Qualifier language, DocumentMetadata.Builder metaBuilder) {
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
        if (dateOfAcceptance != null && StringUtils.isNotBlank(dateOfAcceptance.getValue())
                && fieldApprover.approve(dateOfAcceptance.getDataInfo())) {
            Year year = MetadataConverterUtils.extractYearOrNull(dateOfAcceptance.getValue(), log);
            if (year != null) {
                metaBuilder.setYear(year.getValue());
            }
        }    
    }
    private void handleKeywords(List<Subject> subjectList, DocumentMetadata.Builder metaBuilder) {
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
    
    private static void handlePublicationType(List<Instance> instanceList, DocumentMetadata.Builder metaBuilder) {
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
    private void handleAdditionalIds(Result result, DocumentMetadata.Builder metaBuilder) {
        List<StructuredProperty> pid = MetadataConverterUtils.extractPid(result);
        if (CollectionUtils.isNotEmpty(pid)) {
            Map<CharSequence, CharSequence> additionalIds = pid.stream()
                    .filter(x -> StringUtils.isNotBlank(x.getQualifier().getClassid()))
                    .filter(x -> StringUtils.isNotBlank(x.getValue()))
                    .filter(x -> fieldApprover.approve(x.getDataInfo()))
                    .map(x -> new AbstractMap.SimpleEntry<>(x.getQualifier().getClassid(), x.getValue()))
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue, (val1, val2) -> val1));

            if (MapUtils.isNotEmpty(additionalIds)) {
                metaBuilder.setExternalIdentifiers(additionalIds);
            }  
        }        
    }

    /**
     * Handles datasource identifiers.
     * 
     */
    private static void handleDatasourceIds(OafEntity oafEntity, DocumentMetadata.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(oafEntity.getCollectedfrom())) {
            List<CharSequence> datasourceIds = oafEntity.getCollectedfrom().stream().map(KeyValue::getKey)
                    .collect(Collectors.toList());
            if (!datasourceIds.isEmpty()) {
                metaBuilder.setDatasourceIds(datasourceIds);
            }
        }
    }

    /**
     * Handles persons.
     * 
     * @param relations person result relations
     * @param builder
     * @throws IOException
     */
    private static void handlePersons(Result result, DocumentMetadata.Builder builder)
            throws IOException {
        if (CollectionUtils.isNotEmpty(result.getAuthor())) {
            List<Author> authors = result.getAuthor().stream().map(sourceAuthor -> {
                Author.Builder authorBuilder = Author.newBuilder();
                handleName(sourceAuthor.getName(), authorBuilder);
                handleSurname(sourceAuthor.getSurname(), authorBuilder);
                handleFullName(sourceAuthor.getFullname(), authorBuilder);
                return authorBuilder;
            }).filter(DocumentMetadataConverter::isDataValid).map(Author.Builder::build).collect(Collectors.toList());

            if (!authors.isEmpty()) {
                builder.setAuthors(authors);
            }
        }
    }

    private static void handleName(String firstName, Author.Builder builder) {
        if (StringUtils.isNotBlank(firstName)) {
            builder.setName(firstName);
        }
    }
    
    private static void handleSurname(String surname, Author.Builder builder) {
        if (StringUtils.isNotBlank(surname)) {
            builder.setSurname(surname);
        }
    }
    
    private static void handleFullName(String fullName, Author.Builder builder) {
        if (StringUtils.isNotBlank(fullName)) {
            builder.setFullname(fullName);
        }
    }
    
    private static boolean isDataValid(Author.Builder builder) {
        return builder.hasName() || builder.hasSurname() || builder.hasFullname();
    }


}
