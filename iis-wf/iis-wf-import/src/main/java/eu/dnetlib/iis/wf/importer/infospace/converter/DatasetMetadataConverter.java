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

import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.wf.importer.infospace.approver.FieldApprover;

/**
 * {@link Dataset} containing dataset details to {@link DataSetReference} converter.
 * 
 * @author mhorst
 *
 */
public class DatasetMetadataConverter implements OafEntityToAvroConverter<Result, DataSetReference> {
    
    private static final long serialVersionUID = 3003530002094902347L;

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
    public DataSetReference convert(Result dataset) {
        Objects.requireNonNull(dataset);
        
        DataSetReference.Builder builder = DataSetReference.newBuilder();
        builder.setId(dataset.getId());
        
        createBasicMetadata(dataset, builder);
        handleAdditionalIds(dataset, builder);
        handlePersons(dataset, builder);
        
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
    private void createBasicMetadata(Result sourceResult,
            DataSetReference.Builder metaBuilder) {
        
            handleTitles(sourceResult.getTitle(), metaBuilder);
            handleDescription(sourceResult.getDescription(), metaBuilder);
            handlePublisher(sourceResult.getPublisher(), metaBuilder);
            handleYear(sourceResult.getDateofacceptance(), metaBuilder);
            handleFormats(sourceResult.getFormat(), metaBuilder);
            handleResourceType(sourceResult, metaBuilder);
    }

    private void handleTitles(List<StructuredProperty> titleList, DataSetReference.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(titleList)) {
            metaBuilder.setTitles(titleList.stream().filter(titleProp -> fieldApprover.approve(titleProp.getDataInfo()))
                    .map(StructuredProperty::getValue).collect(Collectors.toList()));
        }
    }
    
    private void handleDescription(List<Field<String>> descriptionList, DataSetReference.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(descriptionList)) {
            descriptionList.stream().filter(x -> fieldApprover.approve(x.getDataInfo()))
                    .filter(x -> StringUtils.isNotBlank(x.getValue()))
                    .filter(x -> !NULL_STRING_VALUE.equals(x.getValue())).findFirst().map(Field::getValue)
                    .ifPresent(metaBuilder::setDescription);
        }
    }
    
    private void handlePublisher(Field<String> publisher, DataSetReference.Builder metaBuilder) {
        if (publisher != null && StringUtils.isNotBlank(publisher.getValue())
                && fieldApprover.approve(publisher.getDataInfo())) {
            metaBuilder.setPublisher(publisher.getValue());
        }    
    }
    
    private void handleYear(Field<String> dateOfAcceptance, DataSetReference.Builder metaBuilder) {
        if (dateOfAcceptance!= null && fieldApprover.approve(dateOfAcceptance.getDataInfo())) {
            Year year = MetadataConverterUtils.extractYearOrNull(dateOfAcceptance.getValue(), log);
            if (year != null) {
                metaBuilder.setPublicationYear(year.toString());
            }
        }    
    }

    private void handleFormats(List<Field<String>> sourceFormats, DataSetReference.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(sourceFormats)) {
            metaBuilder.setFormats(sourceFormats.stream().filter(x -> fieldApprover.approve(x.getDataInfo()))
                    .filter(x -> StringUtils.isNotBlank(x.getValue())).map(Field::getValue)
                    .collect(Collectors.toList()));
        }
    }
    
    private void handleResourceType(Result result, DataSetReference.Builder metaBuilder) {
        if (result.getResourcetype()!=null && StringUtils.isNotBlank(result.getResourcetype().getClassid())) {
            metaBuilder.setResourceTypeValue(result.getResourcetype().getClassid());
        }
    }
    
    /**
     * Handles additional identifiers.
     * 
     */
    private void handleAdditionalIds(OafEntity oafEntity, DataSetReference.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(oafEntity.getPid())) {
            List<StructuredProperty> filtered = oafEntity.getPid().stream()
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
    }

    /**
     * Handles persons.
     * 
     * @param relations person result relations
     * @param builder
     */
    private void handlePersons(Result result, DataSetReference.Builder builder) {
        if (CollectionUtils.isNotEmpty(result.getAuthor())) {
            builder.setCreatorNames(result.getAuthor().stream().filter(x -> StringUtils.isNotBlank(x.getFullname()))
                    .map(Author::getFullname).collect(Collectors.toList()));
        }
    }

}
