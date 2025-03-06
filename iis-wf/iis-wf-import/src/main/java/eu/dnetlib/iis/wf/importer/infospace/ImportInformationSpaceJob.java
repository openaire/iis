package eu.dnetlib.iis.wf.importer.infospace;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE;
import static eu.dnetlib.iis.common.spark.SparkSessionSupport.runWithSparkSession;
import static eu.dnetlib.iis.wf.importer.infospace.ImportInformationSpaceJobUtils.produceGraphIdToObjectStoreIdMapping;

import java.util.Objects;
import java.util.Optional;

import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.SparkConfHelper;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.importer.infospace.approver.DataInfoBasedApprover;
import eu.dnetlib.iis.wf.importer.infospace.approver.ResultApprover;
import eu.dnetlib.iis.wf.importer.infospace.converter.DatasetMetadataConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.DeduplicationMappingConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.DocumentMetadataConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.DocumentToProjectRelationConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.OafEntityToAvroConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.OafRelToAvroConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.OrganizationConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.ProjectConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.ProjectToOrganizationRelationConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.ServiceConverter;
import eu.dnetlib.iis.wf.importer.infospace.truncator.AvroTruncator;
import eu.dnetlib.iis.wf.importer.infospace.truncator.DataSetReferenceAvroTruncator;
import eu.dnetlib.iis.wf.importer.infospace.truncator.DocumentMetadataAvroTruncator;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * 
 * Job responsible for importing InformationSpace entities and relations from the materialized dump.
 * 
 * @author mhorst
 *
 */
public class ImportInformationSpaceJob {

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    public static final Logger log = Logger.getLogger(ImportInformationSpaceJob.class);
    
    
    private static final String FORMAT_PARQUET = "parquet";
    
    private static final String FORMAT_JSON = "json";
    
    
    private static final String REL_TYPE_PROJECT_ORGANIZATION = "projectOrganization";
    
    private static final String REL_TYPE_RESULT_PROJECT = "resultProject";
    
    private static final String REL_TYPE_RESULT_RESULT = "resultResult";
    
    
    private static final String SUBREL_TYPE_PARTICIPATION = "participation";
    
    private static final String SUBREL_TYPE_OUTCOME = "outcome";
    
    private static final String SUBREL_TYPE_DEDUP = "dedup";
    
    
    private static final String REL_NAME_HAS_PARTICIPANT = "hasParticipant";
    
    private static final String REL_NAME_IS_PRODUCED_BY = "isProducedBy";
    
    private static final String REL_NAME_MERGES = "merges";
    

    // reports
    private static final String COUNTER_READ_DOCMETADATA = "import.infoSpace.docMetadata";
    
    private static final String COUNTER_READ_DATASET = "import.infoSpace.dataset";
    
    private static final String COUNTER_READ_PROJECT = "import.infoSpace.project";
    
    private static final String COUNTER_READ_ORGANIZATION = "import.infoSpace.organization";
    
    private static final String COUNTER_READ_SERVICE = "import.infoSpace.service";

    private static final String COUNTER_READ_DOC_PROJ_REFERENCE = "import.infoSpace.docProjectReference";
    
    private static final String COUNTER_READ_DOC_IDENTIFIERMAPPING_DOC_REFERENCE = "import.infoSpace.docIdentifierMappingDocReference";
    
    private static final String COUNTER_READ_PROJ_ORG_REFERENCE = "import.infoSpace.projectOrganizationReference";

    private static final StorageLevel CACHE_STORAGE_DEFAULT_LEVEL = StorageLevel.DISK_ONLY();
    
    public static void main(String[] args) throws Exception {

        ImportInformationSpaceJobParameters params = new ImportInformationSpaceJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        SparkConf conf = SparkConfHelper.withKryo(new SparkConf());
        conf.registerKryoClasses(OafModelUtils.provideOafClasses());

        runWithSparkSession(conf, params.isSparkSessionShared, session -> {
            JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext());

            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            // initializing Oaf model converters
            DataInfoBasedApprover dataInfoBasedApprover = buildApprover(
                    params.skipDeletedByInference, params.trustLevelThreshold, params.inferenceProvenanceBlacklist);

            OafEntityToAvroConverter<Result, DocumentMetadata> documentConverter = new DocumentMetadataConverter(dataInfoBasedApprover);
            OafEntityToAvroConverter<Result, DataSetReference>  datasetConverter = new DatasetMetadataConverter(dataInfoBasedApprover);
            OafEntityToAvroConverter<Organization, eu.dnetlib.iis.importer.schemas.Organization> organizationConverter = new OrganizationConverter();
            OafEntityToAvroConverter<Project, eu.dnetlib.iis.importer.schemas.Project> projectConverter = new ProjectConverter();
			OafEntityToAvroConverter<Datasource, eu.dnetlib.iis.importer.schemas.Service> serviceConverter = new ServiceConverter(
					(WorkflowRuntimeParameters.getValueOrNullIfNotValid(params.eligibleServiceCollectedFromDatasourceId)));

            OafRelToAvroConverter<ProjectToOrganization> projectOrganizationConverter = new ProjectToOrganizationRelationConverter();
            OafRelToAvroConverter<DocumentToProject> docProjectConverter = new DocumentToProjectRelationConverter();
            OafRelToAvroConverter<IdentifierMapping> deduplicationMappingConverter = new DeduplicationMappingConverter();

            DocumentMetadataAvroTruncator documentMetadataAvroTruncator = createDocumentMetadataAvroTruncator(params);
            DataSetReferenceAvroTruncator dataSetReferenceAvroTruncator = createDataSetReferenceAvroTruncator(params);

            String inputFormat = params.inputFormat;

            JavaRDD<eu.dnetlib.dhp.schema.oaf.Organization> sourceOrganization = readGraphTable(session,
                    params.inputRootPath + "/organization", eu.dnetlib.dhp.schema.oaf.Organization.class, inputFormat);
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Project> sourceProject = readGraphTable(session,
                    params.inputRootPath + "/project", eu.dnetlib.dhp.schema.oaf.Project.class, inputFormat);
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Publication> sourcePublication = readGraphTable(session,
                    params.inputRootPath + "/publication", eu.dnetlib.dhp.schema.oaf.Publication.class, inputFormat);
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Dataset> sourceDataset = readGraphTable(session,
                    params.inputRootPath + "/dataset", eu.dnetlib.dhp.schema.oaf.Dataset.class, inputFormat);
            JavaRDD<eu.dnetlib.dhp.schema.oaf.OtherResearchProduct> sourceOtherResearchProduct = readGraphTable(
                    session, params.inputRootPath + "/otherresearchproduct", eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class, inputFormat);
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Software> sourceSoftware = readGraphTable(session,
                    params.inputRootPath + "/software", eu.dnetlib.dhp.schema.oaf.Software.class, inputFormat);
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Relation> sourceRelation = readGraphTable(session,
                    params.inputRootPath + "/relation", eu.dnetlib.dhp.schema.oaf.Relation.class, inputFormat);
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Datasource> sourceDatasource = readGraphTable(session,
                    params.inputRootPath + "/datasource", eu.dnetlib.dhp.schema.oaf.Datasource.class, inputFormat);
            sourceRelation.persist(CACHE_STORAGE_DEFAULT_LEVEL);

            // handling entities
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Dataset> filteredDataset = sourceDataset.filter(dataInfoBasedApprover::approve);
            filteredDataset.persist(CACHE_STORAGE_DEFAULT_LEVEL);

            JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> docMeta = truncateAvro(parseToDocMetaAvro(filteredDataset,
                    sourcePublication, sourceSoftware, sourceOtherResearchProduct, dataInfoBasedApprover, documentConverter),
                    documentMetadataAvroTruncator);
            JavaRDD<eu.dnetlib.iis.importer.schemas.DataSetReference> dataset = truncateAvro(parseResultToAvro(filteredDataset,
                    datasetConverter), dataSetReferenceAvroTruncator);
            JavaRDD<eu.dnetlib.iis.importer.schemas.Project> project = filterAndParseToAvro(sourceProject, dataInfoBasedApprover, projectConverter);
            JavaRDD<eu.dnetlib.iis.importer.schemas.Organization> organization = filterAndParseToAvro(sourceOrganization, dataInfoBasedApprover, organizationConverter);
            JavaRDD<eu.dnetlib.iis.importer.schemas.Service> service = filterAndParseToAvro(sourceDatasource, dataInfoBasedApprover, serviceConverter);
            
            // handling relations
            JavaRDD<DocumentToProject> docProjRelation = filterAndParseRelationToAvro(sourceRelation, dataInfoBasedApprover, docProjectConverter,
                    REL_TYPE_RESULT_PROJECT, SUBREL_TYPE_OUTCOME, REL_NAME_IS_PRODUCED_BY);
            JavaRDD<ProjectToOrganization> projOrgRelation = filterAndParseRelationToAvro(sourceRelation, dataInfoBasedApprover, projectOrganizationConverter,
                    REL_TYPE_PROJECT_ORGANIZATION, SUBREL_TYPE_PARTICIPATION, REL_NAME_HAS_PARTICIPANT);
            JavaRDD<IdentifierMapping> dedupMapping = filterAndParseRelationToAvro(sourceRelation, dataInfoBasedApprover, deduplicationMappingConverter,
                    REL_TYPE_RESULT_RESULT, SUBREL_TYPE_DEDUP, REL_NAME_MERGES);
            
            JavaRDD<IdentifierMapping> identifierMapping = ImportInformationSpaceJobUtils.applyDedupMappingOnTop(
                    produceGraphIdToObjectStoreIdMapping(sourceDataset, sourceOtherResearchProduct, sourcePublication,
                            sourceSoftware, dataInfoBasedApprover, session),
                    dedupMapping);
            
            storeInOutput(sc, docMeta, dataset, project, organization, service, docProjRelation, projOrgRelation, identifierMapping, params);
        });
    }
    
    /**
     * Creates a {@link DocumentMetadataAvroTruncator} from job parameters.
     */
    private static DocumentMetadataAvroTruncator createDocumentMetadataAvroTruncator(ImportInformationSpaceJobParameters params) {
        return DocumentMetadataAvroTruncator.newBuilder()
                .setMaxAbstractLength(params.maxDescriptionLength)
                .setMaxTitleLength(params.maxTitleLength)
                .setMaxAuthorsSize(params.maxAuthorsSize)
                .setMaxAuthorFullnameLength(params.maxAuthorFullnameLength)
                .setMaxKeywordsSize(params.maxKeywordsSize)
                .setMaxKeywordLength(params.maxKeywordLength)
                .build();
    }

    /**
     * Creates a {@link DataSetReferenceAvroTruncator} from job parameters.
     */
    private static DataSetReferenceAvroTruncator createDataSetReferenceAvroTruncator(ImportInformationSpaceJobParameters params) {
        return DataSetReferenceAvroTruncator.newBuilder()
                .setMaxCreatorNamesSize(params.maxAuthorsSize)
                .setMaxCreatorNameLength(params.maxAuthorFullnameLength)
                .setMaxTitlesSize(params.maxTitlesSize)
                .setMaxTitleLength(params.maxTitleLength)
                .setMaxDescriptionLength(params.maxDescriptionLength)
                .build();
    }

    /**
     * Parses given set of RDDs conveying various {@link Result} entities into a single RDD with {@link DocumentMetadata} records,
     * truncating any large entries.
     */
    private static JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> parseToDocMetaAvro(
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Dataset> filteredDataset,
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Publication> sourcePublication,
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Software> sourceSoftware,
            JavaRDD<eu.dnetlib.dhp.schema.oaf.OtherResearchProduct> sourceOtherResearchProduct,
            ResultApprover resultApprover,
            OafEntityToAvroConverter<Result, DocumentMetadata> documentConverter) {
        JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> publicationDocMeta = filterAndParseResultToAvro(
                sourcePublication, resultApprover, documentConverter);

        JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> softwareDocMeta = filterAndParseResultToAvro(
                sourceSoftware, resultApprover, documentConverter);

        JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> orpDocMeta = filterAndParseResultToAvro(
                sourceOtherResearchProduct, resultApprover, documentConverter);

        JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> datasetDocMeta = parseResultToAvro(
                filteredDataset, documentConverter);

        return publicationDocMeta
                .union(softwareDocMeta)
                .union(orpDocMeta)
                .union(datasetDocMeta);
    }
    
    /**
     * Parses given RDD with {@link OafEntity} entities into {@SpecificRecord} avro RDD.
     */
    private static <S extends OafEntity, T extends SpecificRecord>JavaRDD<T> filterAndParseToAvro(JavaRDD<S> source, 
            ResultApprover resultApprover, OafEntityToAvroConverter<S, T> entityConverter) {
        return parseToAvro(source.filter(resultApprover::approve), entityConverter);
    }
    
    /**
     * Converts given RDD with {@link OafEntity} entities into {@link SpecificRecord} avro RDD. Eliminates null records from output.
     */
    private static <S extends OafEntity, T extends SpecificRecord>JavaRDD<T> parseToAvro(JavaRDD<S> source, 
            OafEntityToAvroConverter<S, T> entityConverter) {
        return source.map(entityConverter::convert).filter(Objects::nonNull);
    }
    
    /**
     * Parses given RDD with {@link Result} entities into {@link SpecificRecord} avro RDD.
     */
    private static <T extends SpecificRecord>JavaRDD<T> filterAndParseResultToAvro(JavaRDD<? extends Result> source, 
            ResultApprover resultApprover, OafEntityToAvroConverter<Result, T> entityConverter) {
        return parseResultToAvro(source.filter(resultApprover::approve), entityConverter);
    }

    /**
     * Converts given RDD with {@link Result} entities into {@link SpecificRecord} avro RDD. Eliminates null records from output.
     */
    private static <T extends SpecificRecord>JavaRDD<T> parseResultToAvro(JavaRDD<? extends Result> source, 
            OafEntityToAvroConverter<Result, T> entityConverter) {
        return source.map(entityConverter::convert).filter(Objects::nonNull);
    }
    
    /**
     * Converts given RDD with {@link Result} entities into {@link SpecificRecord} avro RDD. Eliminates null records from output.
     */
    private static <T extends SpecificRecord> JavaRDD<T> filterAndParseRelationToAvro(JavaRDD<Relation> source,
            ResultApprover resultApprover, OafRelToAvroConverter<T> relationConverter, String relType,
            String subRelType, String relClass) {
        return source.filter(x -> acceptRelation(x, relType, subRelType, relClass))
                .filter(x -> resultApprover.approve(x)).map(x -> relationConverter.convert(x));
    }

    /**
     * Truncates an avro type using its avro truncator implementation.
     */
    private static <T extends SpecificRecord> JavaRDD<T> truncateAvro(JavaRDD<T> rdd,
                                                                      AvroTruncator<T> truncator) {
        return rdd.map(truncator::truncate);
    }

    /**
     * Verifies whether given relation should be accepted according to its characteristics.
     */
    private static boolean acceptRelation(Relation relation, String relType, String subRelType, String relClass) {
        return relType.equals(relation.getRelType()) && subRelType.equals(relation.getSubRelType()) && relClass.equals(relation.getRelClass());    
    }
    
    /**
     * Creates data approver.
     */
    private static DataInfoBasedApprover buildApprover(String skipDeletedByInference, String trustLevelThreshold, String inferenceProvenanceBlacklist) {
        Float trustLevelThresholdFloat = Optional.ofNullable(trustLevelThreshold)
                .filter(x -> StringUtils.isNotBlank(x) && !UNDEFINED_NONEMPTY_VALUE.equals(x)).map(Float::parseFloat)
                .orElse(null);
        return new DataInfoBasedApprover(inferenceProvenanceBlacklist, Boolean.parseBoolean(skipDeletedByInference), trustLevelThresholdFloat);
    }
    
    /**
     * Reads graph table according to the input format.
     */
    private static <T extends Oaf> JavaRDD<T> readGraphTable(SparkSession spark, String inputGraphTablePath,
            Class<T> clazz, String format) {
        switch (format) {
        case FORMAT_JSON: {
            return readGraphTableFromTextFile(spark, inputGraphTablePath, clazz);
        }
        case FORMAT_PARQUET: {
            return readGraphTableFromParquet(spark, inputGraphTablePath, clazz);
        }
        default: {
            throw new RuntimeException("unsupported format: " + format);
        }
        }
    }
    
    /**
     * Reads graph table from text file containing JSON records.
     */
    private static <T extends Oaf> JavaRDD<T> readGraphTableFromTextFile(SparkSession spark, String inputGraphTablePath,
            Class<T> clazz) {
        ObjectMapper objectMapper = new ObjectMapper();
        Encoder<T> encoder = Encoders.bean(clazz);
        return spark.read().textFile(inputGraphTablePath).map(
                (MapFunction<String, T>) value -> objectMapper.readValue(value, clazz), encoder).toJavaRDD();
    }
    
    /**
     * Reads graph table from parquet format.
     */
    private static <T extends Oaf> JavaRDD<T> readGraphTableFromParquet(SparkSession spark, String inputGraphTablePath, 
            Class<T> clazz) {
        return spark.read().format("parquet").load(inputGraphTablePath).as(Encoders.bean(clazz)).toJavaRDD();
    }
    
    /**
     * Generates report entries for given counters.
     */
    private static JavaRDD<ReportEntry> generateReportEntries(JavaSparkContext sparkContext, long docMetaCount,
            long datasetCount, long projectCount, long organizationCount, long serviceCount, long docProjCount, long projOrgCount,
            long identifierMappingDocCount) {
        return sparkContext.parallelize(Lists.newArrayList(
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_DOCMETADATA, docMetaCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_DATASET, datasetCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_PROJECT, projectCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_ORGANIZATION, organizationCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_SERVICE, serviceCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_DOC_PROJ_REFERENCE, docProjCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_PROJ_ORG_REFERENCE, projOrgCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_DOC_IDENTIFIERMAPPING_DOC_REFERENCE, identifierMappingDocCount)), 1);
    }
    
    /**
     * Stores given RDDs in HDFS and generates {@link ReportEntry} datastore with counters. 
     */
    private static void storeInOutput(JavaSparkContext sparkContext,
            JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> docMeta,
            JavaRDD<eu.dnetlib.iis.importer.schemas.DataSetReference> dataset,
            JavaRDD<eu.dnetlib.iis.importer.schemas.Project> project,
            JavaRDD<eu.dnetlib.iis.importer.schemas.Organization> organization,
            JavaRDD<eu.dnetlib.iis.importer.schemas.Service> service,
            JavaRDD<DocumentToProject> docProjResultRelation, JavaRDD<ProjectToOrganization> projOrgResultRelation,
            JavaRDD<IdentifierMapping> identifierMapping, ImportInformationSpaceJobParameters jobParams) {

        // caching before calculating counts and writing on HDFS
        docMeta.persist(CACHE_STORAGE_DEFAULT_LEVEL);
        dataset.persist(CACHE_STORAGE_DEFAULT_LEVEL);
        project.persist(CACHE_STORAGE_DEFAULT_LEVEL);
        organization.persist(CACHE_STORAGE_DEFAULT_LEVEL);
        docProjResultRelation.persist(CACHE_STORAGE_DEFAULT_LEVEL);
        projOrgResultRelation.persist(CACHE_STORAGE_DEFAULT_LEVEL);
        identifierMapping.persist(CACHE_STORAGE_DEFAULT_LEVEL);
        
		JavaRDD<ReportEntry> reports = generateReportEntries(sparkContext, docMeta.count(), dataset.count(),
				project.count(), organization.count(), service.count(), docProjResultRelation.count(),
				projOrgResultRelation.count(), identifierMapping.count());

        avroSaver.saveJavaRDD(docMeta, eu.dnetlib.iis.importer.schemas.DocumentMetadata.SCHEMA$,
                outputPathFor(jobParams.outputPath, jobParams.outputNameDocumentMeta));
        avroSaver.saveJavaRDD(dataset, eu.dnetlib.iis.importer.schemas.DataSetReference.SCHEMA$,
                outputPathFor(jobParams.outputPath, jobParams.outputNameDatasetMeta));
        avroSaver.saveJavaRDD(project, eu.dnetlib.iis.importer.schemas.Project.SCHEMA$,
                outputPathFor(jobParams.outputPath, jobParams.outputNameProject));
        avroSaver.saveJavaRDD(organization, eu.dnetlib.iis.importer.schemas.Organization.SCHEMA$,
                outputPathFor(jobParams.outputPath, jobParams.outputNameOrganization));
        avroSaver.saveJavaRDD(service, eu.dnetlib.iis.importer.schemas.Service.SCHEMA$,
                outputPathFor(jobParams.outputPath, jobParams.outputNameService));
        avroSaver.saveJavaRDD(docProjResultRelation, DocumentToProject.SCHEMA$,
                outputPathFor(jobParams.outputPath, jobParams.outputNameDocumentProject));
        avroSaver.saveJavaRDD(projOrgResultRelation, ProjectToOrganization.SCHEMA$,
                outputPathFor(jobParams.outputPath, jobParams.outputNameProjectOrganization));
        avroSaver.saveJavaRDD(identifierMapping, IdentifierMapping.SCHEMA$,
                outputPathFor(jobParams.outputPath, jobParams.outputNameIdentifierMapping));

        avroSaver.saveJavaRDD(reports, ReportEntry.SCHEMA$, jobParams.outputReportPath);
    }
    
    private static String outputPathFor(String path, String subPath) {
        return path + '/' + subPath;
    }

    @Parameters(separators = "=")
    private static class ImportInformationSpaceJobParameters {

        @Parameter(names = "-sharedSparkSession")
        private Boolean isSparkSessionShared = Boolean.FALSE;
        
        @Parameter(names = "-skipDeletedByInference", required = true)
        private String skipDeletedByInference;
        
        @Parameter(names = "-trustLevelThreshold", required = true)
        private String trustLevelThreshold;
        
        @Parameter(names = "-inferenceProvenanceBlacklist", required = true)
        private String inferenceProvenanceBlacklist;
        
        @Parameter(names = "-inputRootPath", required = true)
        private String inputRootPath;
        
        @Parameter(names = "-inputFormat", required = true)
        private String inputFormat;

        @Parameter(names = "-outputPath", required = true)
        private String outputPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
        
        @Parameter(names = "-outputNameDocumentMeta", required = true)
        private String outputNameDocumentMeta;

        @Parameter(names = "-outputNameDatasetMeta", required = true)
        private String outputNameDatasetMeta;
        
        @Parameter(names = "-outputNameDocumentProject", required = true)
        private String outputNameDocumentProject;
        
        @Parameter(names = "-outputNameProject", required = true)
        private String outputNameProject;
        
        @Parameter(names = "-outputNameIdentifierMapping", required = true)
        private String outputNameIdentifierMapping;
        
        @Parameter(names = "-outputNameOrganization", required = true)
        private String outputNameOrganization;
        
        @Parameter(names = "-outputNameProjectOrganization", required = true)
        private String outputNameProjectOrganization;

        @Parameter(names = "-outputNameService", required = true)
        private String outputNameService;
        
        @Parameter(names = "-maxDescriptionLength", required = true)
        private Integer maxDescriptionLength;

        @Parameter(names = "-maxTitlesSize", required = true)
        private Integer maxTitlesSize;

        @Parameter(names = "-maxTitleLength", required = true)
        private Integer maxTitleLength;

        @Parameter(names = "-maxAuthorsSize", required = true)
        private Integer maxAuthorsSize;

        @Parameter(names = "-maxAuthorFullnameLength", required = true)
        private Integer maxAuthorFullnameLength;

        @Parameter(names = "-maxKeywordsSize", required = true)
        private Integer maxKeywordsSize;

        @Parameter(names = "-maxKeywordLength", required = true)
        private Integer maxKeywordLength;
        
        @Parameter(names = "-eligibleServiceCollectedFromDatasourceId", required = true)
        private String eligibleServiceCollectedFromDatasourceId;
    }
    
}
