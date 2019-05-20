package eu.dnetlib.iis.wf.importer.infospace.converter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

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
    
    protected static final Logger log = Logger.getLogger(DatasetMetadataConverter.class);

    private static final String NULL_STRING_VALUE = "null";


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
        this.fieldApprover = Preconditions.checkNotNull(fieldApprover);
    }

    // ------------------------ LOGIC --------------------------
    
    @Override
    public DataSetReference convert(OafEntity oafEntity) throws IOException {
        Preconditions.checkNotNull(oafEntity);
        
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
        
        //checking whether required fields were set
        if (StringUtils.isNotBlank(builder.getReferenceType()) && StringUtils.isNotBlank(builder.getIdForGivenType())) {
            return builder.build();    
        } else {
            log.warn("referenceType unset due to missing DOI, skipping...");
            return null;
        }
    }

    // ------------------------ PRIVATE --------------------------
    
    /**
     * Creates basic metadata object.
     * 
     * @param sourceResult
     * @param metaBuilder
     * @return basic metadata object
     */
    private DataSetReference.Builder createBasicMetadata(ResultProtos.Result sourceResult,
            DataSetReference.Builder metaBuilder) {
        
        if (sourceResult.hasMetadata()) {
            handleTitles(sourceResult.getMetadata().getTitleList(), metaBuilder);
            handleDescription(sourceResult.getMetadata().getDescriptionList(), metaBuilder);
            handlePublisher(sourceResult.getMetadata().getPublisher(), metaBuilder);
            handleYear(sourceResult.getMetadata().getDateofacceptance(), metaBuilder);
            handleFormats(sourceResult.getMetadata().getFormatList(), metaBuilder);
            handleResourceType(sourceResult.getMetadata(), metaBuilder);
        }
        
        return metaBuilder;
    }

    private void handleTitles(List<StructuredProperty> titleList, DataSetReference.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(titleList)) {
            List<CharSequence> titles = new ArrayList<CharSequence>();
            
            for (StructuredProperty titleProp : titleList) {
                if (fieldApprover.approve(titleProp.getDataInfo())) {
                    titles.add(titleProp.getValue());
                }
            }
            
            metaBuilder.setTitles(titles);
        }
    }
    
    private void handleDescription(List<StringField> descriptionList, DataSetReference.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(descriptionList)) {
            for (StringField currentDescription : descriptionList) {
                if (fieldApprover.approve(currentDescription.getDataInfo()) 
                        && StringUtils.isNotBlank(currentDescription.getValue())
                        && !NULL_STRING_VALUE.equals(currentDescription.getValue())) {
                    metaBuilder.setDescription(currentDescription.getValue());
                    break;
                }
            }
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
            String yearValue = MetadataConverterUtils.extractYear(dateOfAcceptance.getValue());
            if (yearValue != null) {
                metaBuilder.setPublicationYear(yearValue);
            }
        }    
    }

    private void handleFormats(List<StringField> sourceFormats, DataSetReference.Builder metaBuilder) {
        if (CollectionUtils.isNotEmpty(sourceFormats)) {
            List<CharSequence> resultFormats = new ArrayList<CharSequence>();
            
            for (StringField sourceFormat : sourceFormats) {
                if (fieldApprover.approve(sourceFormat.getDataInfo())
                        && StringUtils.isNotBlank(sourceFormat.getValue())) {
                    resultFormats.add(sourceFormat.getValue());
                }
            }
            
            metaBuilder.setFormats(resultFormats);
        }
    }
    
    private void handleResourceType(Metadata metadata, DataSetReference.Builder metaBuilder) {
        if (metadata.getResulttype()!=null && StringUtils.isNotBlank(metadata.getResulttype().getClassid())
                && fieldApprover.approve(metadata.getResulttype().getDataInfo())) {
            metaBuilder.setResourceTypeClass(metadata.getResulttype().getClassid());
        }
        
        if (metadata.getResourcetype()!=null && StringUtils.isNotBlank(metadata.getResourcetype().getClassid())
                && fieldApprover.approve(metadata.getResourcetype().getDataInfo())) {
            metaBuilder.setResourceTypeValue(metadata.getResourcetype().getClassid());
        }
    }
    
    /**
     * Handles additional identifiers.
     * 
     */
    private DataSetReference.Builder handleAdditionalIds(OafEntity oafEntity, DataSetReference.Builder metaBuilder) {
        // setting additional identifiers
        Map<CharSequence, CharSequence> additionalIds = new HashMap<CharSequence, CharSequence>();
        if (CollectionUtils.isNotEmpty(oafEntity.getPidList())) {
            for (StructuredProperty currentPid : oafEntity.getPidList()) {
                if (StringUtils.isNotBlank(currentPid.getQualifier().getClassid()) 
                        && StringUtils.isNotBlank(currentPid.getValue()) 
                        && fieldApprover.approve(currentPid.getDataInfo())) {
                    additionalIds.put(currentPid.getQualifier().getClassid(), currentPid.getValue());
                    if (ID_TYPE_DOI_LOWERCASED.equalsIgnoreCase(currentPid.getQualifier().getClassid())) {
                        metaBuilder.setReferenceType(ID_TYPE_DOI_LOWERCASED);
                        metaBuilder.setIdForGivenType(currentPid.getValue());
                    }
                }
            }
        }
        if (!additionalIds.isEmpty()) {
            metaBuilder.setAlternateIdentifiers(additionalIds);
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
    private DataSetReference.Builder handlePersons(ResultProtos.Result result, DataSetReference.Builder builder)
            throws IOException {
        
        if (result.getMetadata() != null) {
            List<CharSequence> creatorNames = new ArrayList<>();
            for (eu.dnetlib.data.proto.FieldTypeProtos.Author sourceAuthor : result.getMetadata().getAuthorList()) {
                if (StringUtils.isNotBlank(sourceAuthor.getFullname())) {
                    //fullname is never empty in IS dump
                    creatorNames.add(sourceAuthor.getFullname());
                }
            }
            if (!creatorNames.isEmpty()) {
                builder.setCreatorNames(creatorNames);
            }
        }
        
        return builder;
    }

}
