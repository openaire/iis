package eu.dnetlib.iis.wf.importer.infospace;

import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.HBASE_ENCODING;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_INFERENCE_PROVENANCE_BLACKLIST;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_MERGE_BODY_WITH_UPDATES;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_SKIP_DELETED_BY_INFERENCE;
import static eu.dnetlib.iis.wf.importer.ImportWorkflowRuntimeParameters.IMPORT_TRUST_LEVEL_THRESHOLD;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.googlecode.protobuf.format.JsonFormat;
import com.googlecode.protobuf.format.JsonFormat.ParseException;

import eu.dnetlib.data.mapreduce.util.OafRelDecoder;
import eu.dnetlib.data.proto.DedupProtos.Dedup;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.ProjectOrganizationProtos.ProjectOrganization;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
import eu.dnetlib.data.proto.ResultProjectProtos.ResultProject.Outcome;
import eu.dnetlib.data.proto.TypeProtos.Type;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.common.javamapreduce.MultipleOutputs;
import eu.dnetlib.iis.common.utils.ByteArrayUtils;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.importer.OafHelper;
import eu.dnetlib.iis.wf.importer.infospace.approver.DataInfoBasedApprover;
import eu.dnetlib.iis.wf.importer.infospace.approver.ResultApprover;
import eu.dnetlib.iis.wf.importer.infospace.converter.DeduplicationMappingConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.DocumentMetadataConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.DocumentToProjectConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.InfoSpaceRecordUtils;
import eu.dnetlib.iis.wf.importer.infospace.converter.OafEntityToAvroConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.OafRelToAvroConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.OrganizationConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.PersonConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.ProjectConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.ProjectToOrganizationConverter;

/**
 * InformationSpace reducer phase importing {@link InfoSpaceRecord}s grouped by row identifier.
 * Emits entities and relations as avro records written to mulitple outputs. 
 * Each output is associated with individual entity or relation type.
 * 
 * @author mhorst
 *
 */
public class ImportInformationSpaceReducer
        extends Reducer<ImmutableBytesWritable, InfoSpaceRecord, NullWritable, NullWritable> {

    // context property names
    
    private static final Logger log = Logger.getLogger(ImportInformationSpaceReducer.class);
    
    private static final String OUTPUT_NAME_DOCUMENT_META = "output.name.document_meta";

    private static final String OUTPUT_NAME_DOCUMENT_PROJECT = "output.name.document_project";

    private static final String OUTPUT_NAME_PROJECT = "output.name.project";

    private static final String OUTPUT_NAME_PERSON = "output.name.person";

    private static final String OUTPUT_NAME_DEDUP_MAPPING = "output.name.dedup_mapping";

    private static final String OUTPUT_NAME_ORGANIZATION = "output.name.organization";

    private static final String OUTPUT_NAME_PROJECT_ORGANIZATION = "output.name.project_organization";

    // column family names
    
    private final String projOrgColumnFamily = OafRelDecoder.getCFQ(RelType.projectOrganization,
            SubRelType.participation, ProjectOrganization.Participation.RelName.hasParticipant.toString());
    
    private final String resProjColumnFamily = OafRelDecoder.getCFQ(RelType.resultProject, 
            SubRelType.outcome, Outcome.RelName.isProducedBy.toString());
    
    private final String dedupMappingColumnFamily = OafRelDecoder.getCFQ(RelType.resultResult, 
            SubRelType.dedup, Dedup.RelName.merges.toString());

    // output names
    
    private String outputNameDocumentMeta;

    private String outputNameDocumentProject;

    private String outputNameProject;

    private String outputNamePerson;

    private String outputNameDedupMapping;

    private String outputNameOrganization;

    private String outputNameProjectOrganization;

    // converters
    
    private DocumentMetadataConverter docMetaConverter;

    private DocumentToProjectConverter docProjectConverter;

    private DeduplicationMappingConverter deduplicationMappingConverter;

    private PersonConverter personConverter;

    private ProjectConverter projectConverter;

    private OrganizationConverter organizationConverter;

    private ProjectToOrganizationConverter projectOrganizationConverter;

    // others
    
    private String encoding = HBaseConstants.STATIC_FIELDS_ENCODING_UTF8;
    
    private MultipleOutputs mos;
    
    private ResultApprover resultApprover;
    
    /**
     * Flag indicating {@link Oaf} retrieved from body column family should be merged with all update collumns. 
     * Set to false by default.
     */
    private boolean mergeBodyWithUpdates;
    
    // ------------------------ LOGIC --------------------------
    
    @Override
    public void setup(Context context) {
        setOutputDirs(context);

        if (context.getConfiguration().get(HBASE_ENCODING) != null) {
            encoding = context.getConfiguration().get(HBASE_ENCODING);
        }
        mergeBodyWithUpdates = context.getConfiguration().get(IMPORT_MERGE_BODY_WITH_UPDATES) != null
                ? Boolean.valueOf(context.getConfiguration().get(IMPORT_MERGE_BODY_WITH_UPDATES)) : false;

        DataInfoBasedApprover dataInfoBasedApprover = buildApprover(context);
        this.resultApprover = dataInfoBasedApprover;

        // initializing converters
        docMetaConverter = new DocumentMetadataConverter(this.resultApprover, dataInfoBasedApprover);
        deduplicationMappingConverter = new DeduplicationMappingConverter();
        docProjectConverter = new DocumentToProjectConverter();
        personConverter = new PersonConverter();
        projectConverter = new ProjectConverter();
        organizationConverter = new OrganizationConverter();
        projectOrganizationConverter = new ProjectToOrganizationConverter();
    }
    
    @Override
    public void cleanup(Context context) 
            throws IOException, InterruptedException {
        try {
            super.cleanup(context);
        } finally {
            mos.close();
        }
    }
    
    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<InfoSpaceRecord> values, Context context)
            throws IOException, InterruptedException {
        byte[] idBytes = key.get();
        Map<String, List<QualifiedOafJsonRecord>> mappedRecords = InfoSpaceRecordUtils.mapByColumnFamily(values);
        
        if (ByteArrayUtils.startsWith(idBytes, HBaseConstants.ROW_PREFIX_RESULT)) {
            handleResult(idBytes, mappedRecords);
        } else if (ByteArrayUtils.startsWith(idBytes, HBaseConstants.ROW_PREFIX_PERSON)) {
            handleEntity(idBytes, mappedRecords.get(Type.person.name()), personConverter, outputNamePerson);
        } else if (ByteArrayUtils.startsWith(idBytes, HBaseConstants.ROW_PREFIX_PROJECT)) {
            handleEntity(idBytes, mappedRecords.get(Type.project.name()), projectConverter, outputNameProject,
                    new RelationConversionDTO<ProjectToOrganization>(mappedRecords.get(projOrgColumnFamily),
                            projectOrganizationConverter, outputNameProjectOrganization));
        } else if (ByteArrayUtils.startsWith(idBytes, HBaseConstants.ROW_PREFIX_ORGANIZATION)) {
            handleEntity(idBytes, mappedRecords.get(Type.organization.name()), organizationConverter, outputNameOrganization);
        }
    }
    
    // ------------------------ PRIVATE --------------------------
    
    /**
     * Sets output directories.
     * @param context hadoop context providing directories output names
     */
    private void setOutputDirs(Context context) {
        Preconditions.checkNotNull(outputNameDocumentMeta = context.getConfiguration().get(OUTPUT_NAME_DOCUMENT_META),
                "document metadata output name not provided!");
        Preconditions.checkNotNull(outputNameDocumentProject = context.getConfiguration().get(OUTPUT_NAME_DOCUMENT_PROJECT),
                "document project relation output name not provided!");
        Preconditions.checkNotNull(outputNameProject = context.getConfiguration().get(OUTPUT_NAME_PROJECT),
                "project output name not provided!");
        Preconditions.checkNotNull(outputNamePerson = context.getConfiguration().get(OUTPUT_NAME_PERSON),
                "person output name not provided!");
        Preconditions.checkNotNull(outputNameDedupMapping = context.getConfiguration().get(OUTPUT_NAME_DEDUP_MAPPING),
                "deduplication mapping output name not provided!");
        Preconditions.checkNotNull(outputNameOrganization = context.getConfiguration().get(OUTPUT_NAME_ORGANIZATION),
                "organization output name not provided!");
        Preconditions.checkNotNull(outputNameProjectOrganization = context.getConfiguration().get(OUTPUT_NAME_PROJECT_ORGANIZATION),
                "project to organization output name not provided!");
        mos = new MultipleOutputs(context);
    }
    
    /**
     * Creates data approver.
     */
    private DataInfoBasedApprover buildApprover(Context context) {
        boolean skipDeletedByInference = true;
        String skipDeletedByInferenceParamValue = ImportInformationSpaceUtils.getParamValue(IMPORT_SKIP_DELETED_BY_INFERENCE, context);
        if (skipDeletedByInferenceParamValue != null) {
            skipDeletedByInference = Boolean.valueOf(skipDeletedByInferenceParamValue);
        }
        
        Float trustLevelThreshold = null;
        String trustLevelThresholdParamValue = ImportInformationSpaceUtils.getParamValue(IMPORT_TRUST_LEVEL_THRESHOLD, context);
        if (trustLevelThresholdParamValue != null) {
            trustLevelThreshold = Float.valueOf(trustLevelThresholdParamValue);
        }
        
        return new DataInfoBasedApprover(ImportInformationSpaceUtils.getParamValue(IMPORT_INFERENCE_PROVENANCE_BLACKLIST, context), 
                skipDeletedByInference, trustLevelThreshold);
    }
    
    /**
     * Handles result entity with relations.
     * 
     */
    private void handleResult(final byte[] idBytes, Map<String, List<QualifiedOafJsonRecord>> mappedRecords)
            throws InterruptedException, IOException {
        Oaf oafObj = buildOafObject(mappedRecords.get(Type.result.name()));
        if (oafObj == null) {
            log.error("missing 'body' qualifier value for record " + new String(idBytes, encoding));
            return;
        }
        if (resultApprover.approve(oafObj)) {
            DocumentMetadata docMeta = docMetaConverter.convert(oafObj.getEntity(), mappedRecords);
            if (docMeta!=null) {
                mos.write(outputNameDocumentMeta, new AvroKey<DocumentMetadata>(docMeta));    
            }
            // hadling project relations
            handleRelation(mappedRecords.get(resProjColumnFamily), docProjectConverter, outputNameDocumentProject);
            // handling deduplication relations, required for contents deduplication and identifiers translation
            handleRelation(mappedRecords.get(dedupMappingColumnFamily), deduplicationMappingConverter, outputNameDedupMapping);
        }
    }
    
    /**
     * Handles relations by converting them to avro format and writing to output. 
     */
    private <T extends SpecificRecord> void handleRelation(List<QualifiedOafJsonRecord> relations, 
            OafRelToAvroConverter<T> converter, String outputName) throws InterruptedException, IOException {
        if (!CollectionUtils.isEmpty(relations)) {
            for (QualifiedOafJsonRecord relationRecord : relations) {
                Oaf relOaf = OafHelper.buildOaf(relationRecord.getOafJson());
                if (resultApprover.approve(relOaf)) {
                    T avroRelation = converter.convert(relOaf.getRel());
                    if (avroRelation!=null) {
                        mos.write(outputName, new AvroKey<T>(avroRelation));    
                    }
                }
            }
        }
    }
    
    /**
     * Handles entity by converting it to avro format and writing to output.
     * Each entity may constit of many parts: body with updates.
     * Optional relations are expected as the last parameters.
     */
    private <T extends SpecificRecord> void handleEntity(final byte[] idBytes, 
            List<QualifiedOafJsonRecord> bodyParts, OafEntityToAvroConverter<T> converter, String outputName,
            RelationConversionDTO<?>... relationConversionDTO) throws InterruptedException, IOException {
        Oaf oafObj = buildOafObject(bodyParts);
        if (oafObj == null) {
            log.error("missing 'body' qualifier value for record " + new String(idBytes, encoding));
            return;
        }
        if (resultApprover.approve(oafObj)) {
            T avroEntity = converter.convert(oafObj.getEntity());
            if (avroEntity != null) {
                mos.write(outputName, new AvroKey<T>(avroEntity));
            }
            // handing relations
            if (relationConversionDTO!=null) {
                for (RelationConversionDTO<?> currentDTO : relationConversionDTO) {
                    handleRelation(currentDTO.getOafJsonParts(), currentDTO.getConverter(), currentDTO.getOutputName());
                }
            }
        }
    }
    
    /**
     * Builds {@link Oaf} object from JSON body represetation and updates.
     * 
     * @param bodyRecords body records with optional updates
     * @return {@link Oaf} object built from JSON representation or null when body was undefined
     * @throws UnsupportedEncodingException
     * @throws ParseException 
     */
    private Oaf buildOafObject(List<QualifiedOafJsonRecord> bodyRecords) throws UnsupportedEncodingException, ParseException {
        if (bodyRecords !=null) {
            OafBodyWithOrderedUpdates bodyWithUpdates = new OafBodyWithOrderedUpdates(bodyRecords);
            if (bodyWithUpdates.getBody() != null) {
                Oaf.Builder oafBuilder = Oaf.newBuilder();
                JsonFormat.merge(bodyWithUpdates.getBody(), oafBuilder);
                if (this.mergeBodyWithUpdates) {
                    for (String oafUpdate : bodyWithUpdates.getOrderedUpdates()) {
                        JsonFormat.merge(oafUpdate, oafBuilder);
                    }
                }
                return oafBuilder.build();
            }    
        }
        return null;
    }
    
    // ------------------------ INNER CLASSES --------------------------
       
    /**
     * Encapsulates set of parameters required to perform relation conversion.
     *
     * @param <T>
     */
    class RelationConversionDTO <T extends SpecificRecord> {
        
        private List<QualifiedOafJsonRecord> oafJsonParts;
        
        private OafRelToAvroConverter<T> converter;

        private String outputName;
        
        // ------------------------ CONSTRUCTORS --------------------------
        
        public RelationConversionDTO(List<QualifiedOafJsonRecord> oafJsonParts, OafRelToAvroConverter<T> converter, String outputName) {
            this.oafJsonParts = oafJsonParts;
            this.converter = converter;
            this.outputName = outputName;
        }
        
        // ------------------------ GETTERS --------------------------
        
        List<QualifiedOafJsonRecord> getOafJsonParts() {
            return oafJsonParts;
        }
        
        public OafRelToAvroConverter<T> getConverter() {
            return converter;
        }

        String getOutputName() {
            return outputName;
        }
    }
    
}