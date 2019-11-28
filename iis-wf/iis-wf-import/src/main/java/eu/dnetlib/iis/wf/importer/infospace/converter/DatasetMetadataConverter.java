package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.time.Year;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import eu.dnetlib.data.proto.FieldTypeProtos.Author;
import eu.dnetlib.data.proto.FieldTypeProtos.StringField;
import eu.dnetlib.data.proto.FieldTypeProtos.StructuredProperty;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.ResultProtos;
import eu.dnetlib.data.proto.ResultProtos.Result.Metadata;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;

/**
 * {@link OafEntity} containing document details to {@link DataSetReference} converter.
 * 
 * @author mhorst
 *
 */
public class DatasetMetadataConverter implements OafEntityToAvroConverter<DataSetReference> {
    
    private static final String ID_TYPE_DOI_LOWERCASED = "doi";
    
    private static final Logger log = Logger.getLogger(DatasetMetadataConverter.class);

    private static final String NULL_STRING_VALUE = "null";
    
    private static final String REFERENCE_TYPE_UNSPECIFIED = "unspecified";


    /**
     * Field approver to be used when validating inferred fields.
     */
    private FieldApprover fieldApprover;

    // ------------------------ CONSTRUCTORS --------------------------
    
    /**
     * 
     * @param fieldApprover approves fields
     */
    public DatasetMetadataConverter(FieldApprover fieldApprover) {
        this.fieldApprover = Objects.requireNonNull(fieldApprover);
    }

    // ------------------------ LOGIC --------------------------
    
    @Override
    public DataSetReference convert(OafEntity oafEntity) {
        Objects.requireNonNull(oafEntity);
        
        if (!oafEntity.hasResult()) {
            log.error("skipping: no result object for id " + oafEntity.getId());
            return null;
        }
        DataSetReference.Builder builder = DataSetReference.newBuilder();
        builder.setId(oafEntity.getId());
        
        ResultProtos.Result sourceResult = oafEntity.getResult();
        createBasicMetadata(sourceResult, builder);
        handleAdditionalIds(oafEntity, builder);
        handlePersons(sourceResult, builder);
        
        //checking whether required fields (according to avro schema) were set
        if (builder.getReferenceType() == null) {
            builder.setReferenceType(REFERENCE_TYPE_UNSPECIFIED);
        }
        if (builder.getIdForGivenType() == null) {
            builder.setIdForGivenType("");
        }
        
        return builder.build();
    }

    // ------------------------ PRIVATE --------------------------
    
    /**
     * Creates basic metadata object.
     * 
     * @param sourceResult
     * @param metaBuilder
     */
    private void createBasicMetadata(ResultProtos.Result sourceResult,
            DataSetReference.Builder metaBuilder) {
        
        if (sourceResult.hasMetadata()) {
            handleTitles(sourceResult.getMetadata().getTitleList(), metaBuilder);
            handleDescription(sourceResult.getMetadata().getDescriptionList(), metaBuilder);
            handlePublisher(sourceResult.getMetadata().getPublisher(), metaBuilder);
            handleYear(sourceResult.getMetadata().getDateofacceptance(), metaBuilder);
            handleFormats(sourceResult.getMetadata().getFormatList(), metaBuilder);
            handleResourceType(sourceResult.getMetadata(), metaBuilder);
        }
    }

    private void handleTitles(List<StructuredProperty> titleList, DataSetReference.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(titleList)) {
            metaBuilder.setTitles(titleList.stream().filter(titleProp -> fieldApprover.approve(titleProp.getDataInfo()))
                    .map(StructuredProperty::getValue).collect(Collectors.toList()));
        }
    }
    
    private void handleDescription(List<StringField> descriptionList, DataSetReference.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(descriptionList)) {
            descriptionList.stream().filter(x -> fieldApprover.approve(x.getDataInfo()))
                    .filter(x -> StringUtils.isNotBlank(x.getValue()))
                    .filter(x -> !NULL_STRING_VALUE.equals(x.getValue())).findFirst().map(StringField::getValue)
                    .ifPresent(metaBuilder::setDescription);
        }
    }
    
    private void handlePublisher(StringField publisher, DataSetReference.Builder metaBuilder) {
        if (StringUtils.isNotBlank(publisher.getValue())
                && fieldApprover.approve(publisher.getDataInfo())) {
            metaBuilder.setPublisher(publisher.getValue());
        }    
    }
    
    private void handleYear(StringField dateOfAcceptance, DataSetReference.Builder metaBuilder) {
        if (fieldApprover.approve(dateOfAcceptance.getDataInfo())) {
            Year year = MetadataConverterUtils.extractYearOrNull(dateOfAcceptance.getValue(), log);
            if (year != null) {
                metaBuilder.setPublicationYear(year.toString());
            }
        }    
    }

    private void handleFormats(List<StringField> sourceFormats, DataSetReference.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(sourceFormats)) {
            metaBuilder.setFormats(sourceFormats.stream().filter(x -> fieldApprover.approve(x.getDataInfo()))
                    .filter(x -> StringUtils.isNotBlank(x.getValue())).map(StringField::getValue)
                    .collect(Collectors.toList()));
        }
    }
    
    private void handleResourceType(Metadata metadata, DataSetReference.Builder metaBuilder) {
        if (metadata.getResourcetype()!=null && StringUtils.isNotBlank(metadata.getResourcetype().getClassid())) {
            metaBuilder.setResourceTypeValue(metadata.getResourcetype().getClassid());
        }
    }
    
    /**
     * Handles additional identifiers.
     * 
     */
    private void handleAdditionalIds(OafEntity oafEntity, DataSetReference.Builder metaBuilder) {
        List<StructuredProperty> filtered = oafEntity.getPidList().stream()
                .filter(x -> StringUtils.isNotBlank(x.getQualifier().getClassid()))
                .filter(x -> StringUtils.isNotBlank(x.getValue()))
                .filter(x -> fieldApprover.approve(x.getDataInfo()))
                .collect(Collectors.toList());
        Map<CharSequence, CharSequence> additionalIds = filtered.stream()
                .collect(Collectors.toMap(k -> k.getQualifier().getClassid(), v -> v.getValue(), (val1, val2) -> val1));
        Optional.ofNullable(
                filtered.stream().filter(x -> ID_TYPE_DOI_LOWERCASED.equalsIgnoreCase(x.getQualifier().getClassid()))
                        .collect(Collectors.toCollection(LinkedList::new)).peekLast())
                .ifPresent(last -> {
                    metaBuilder.setReferenceType(ID_TYPE_DOI_LOWERCASED);
                    metaBuilder.setIdForGivenType(last.getValue());
                });

        if (!additionalIds.isEmpty()) {
            metaBuilder.setAlternateIdentifiers(additionalIds);
            if (metaBuilder.getReferenceType() == null) {
                // setting other identifier pair when DOI was not present
                Entry<CharSequence, CharSequence> firstIdsPair = additionalIds.entrySet().iterator().next();
                metaBuilder.setReferenceType(firstIdsPair.getKey());
                metaBuilder.setIdForGivenType(firstIdsPair.getValue());
            }
        }
    }

    /**
     * Handles persons.
     * 
     * @param relations person result relations
     * @param builder
     */
    private void handlePersons(ResultProtos.Result result, DataSetReference.Builder builder) {
        if (result.getMetadata() != null) {
            builder.setCreatorNames(result.getMetadata().getAuthorList().stream()
                    .filter(x -> StringUtils.isNotBlank(x.getFullname()))
                    .map(Author::getFullname)
                    .collect(Collectors.toList()));
        }
    }

}
