package eu.dnetlib.iis.wf.importer.infospace;

import static eu.dnetlib.iis.common.WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE;

import java.util.Objects;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.Country;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.ExternalReference;
import eu.dnetlib.dhp.schema.oaf.ExtraInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.GeoLocation;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.OAIProvenance;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.OriginDescription;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.importer.infospace.approver.DataInfoBasedApprover;
import eu.dnetlib.iis.wf.importer.infospace.approver.ResultApprover;
import eu.dnetlib.iis.wf.importer.infospace.converter.DatasetMetadataConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.DeduplicationMappingConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.DocumentMetadataConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.DocumentToProjectRelationConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.OrganizationConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.ProjectConverter;
import eu.dnetlib.iis.wf.importer.infospace.converter.ProjectToOrganizationRelationConverter;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

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

    private static final String COUNTER_READ_DOC_PROJ_REFERENCE = "import.infoSpace.docProjectReference";
    
    private static final String COUNTER_READ_DOC_DEDUP_DOC_REFERENCE = "import.infoSpace.docDedupDocReference";
    
    private static final String COUNTER_READ_PROJ_ORG_REFERENCE = "import.infoSpace.projectOrganizationReference";

    
    public static void main(String[] args) throws Exception {

        ImportInformationSpaceJobParameters params = new ImportInformationSpaceJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        conf.registerKryoClasses(provideOafClasses());
        
//      SparkSession based
        SparkSession session = null;
        try {
            // initializing Oaf model converters
            DataInfoBasedApprover dataInfoBasedApprover = buildApprover(
                    params.skipDeletedByInference, params.trustLevelThreshold, params.inferenceProvenanceBlacklist);
            ResultApprover resultApprover = dataInfoBasedApprover;
            
            DocumentMetadataConverter documentConverter = new DocumentMetadataConverter(dataInfoBasedApprover);
            DatasetMetadataConverter datasetConverter = new DatasetMetadataConverter(dataInfoBasedApprover);
            OrganizationConverter organizationConverter = new OrganizationConverter();
            ProjectConverter projectConverter = new ProjectConverter();
            
            ProjectToOrganizationRelationConverter projectOrganizationConverter = new ProjectToOrganizationRelationConverter();
            DocumentToProjectRelationConverter docProjectConverter = new DocumentToProjectRelationConverter();
            DeduplicationMappingConverter deduplicationMappingConverter = new DeduplicationMappingConverter();
       
            session = SparkSession.builder().config(conf).getOrCreate();
            JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext()); 
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);
            
            Dataset<eu.dnetlib.dhp.schema.oaf.Organization> sourceOrganization = readGraphTableFromSession(session,
                    params.inputRootPath + "/organization", eu.dnetlib.dhp.schema.oaf.Organization.class);
            Dataset<eu.dnetlib.dhp.schema.oaf.Project> sourceProject = readGraphTableFromSession(session,
                    params.inputRootPath + "/project", eu.dnetlib.dhp.schema.oaf.Project.class);
            Dataset<eu.dnetlib.dhp.schema.oaf.Publication> sourcePublication = readGraphTableFromSession(session,
                    params.inputRootPath + "publication", eu.dnetlib.dhp.schema.oaf.Publication.class);
            Dataset<eu.dnetlib.dhp.schema.oaf.Dataset> sourceDataset = readGraphTableFromSession(session,
                    params.inputRootPath + "/dataset", eu.dnetlib.dhp.schema.oaf.Dataset.class);
            Dataset<eu.dnetlib.dhp.schema.oaf.OtherResearchProduct> sourceOtherResearchProduct = readGraphTableFromSession(
                    session, params.inputRootPath + "/otherresearchproduct",
                    eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class);
            Dataset<eu.dnetlib.dhp.schema.oaf.Software> sourceSoftware = readGraphTableFromSession(session,
                    params.inputRootPath + "/software", eu.dnetlib.dhp.schema.oaf.Software.class);
            Dataset<eu.dnetlib.dhp.schema.oaf.Relation> sourceRelation = readGraphTableFromSession(session,
                    params.inputRootPath + "/relation", eu.dnetlib.dhp.schema.oaf.Relation.class);
            sourceRelation.cache();

            
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Dataset> approvedDataset = sourceDataset.toJavaRDD()
                    .filter(x -> resultApprover.approve(x));
//          TODO should we keep filtering by resultType=dataset or can we trust the full dataset subdirectory contents
            approvedDataset.cache();

            JavaRDD<eu.dnetlib.iis.importer.schemas.DataSetReference> resultDataset = approvedDataset
                    .map(x -> datasetConverter.convert(x)).filter(x -> x != null);
            resultDataset.cache();
            
            JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> resultDocMeta = null;
            {
                JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> publicationDocMeta = parseToDocMeta(
                        sourcePublication, resultApprover, documentConverter);
                
                JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> softwareDocMeta = parseToDocMeta(
                        sourceSoftware, resultApprover, documentConverter);

                JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> orpDocMeta = parseToDocMeta(
                        sourceOtherResearchProduct, resultApprover, documentConverter);

                JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> datasetDocMeta = approvedDataset
                        .map(x -> documentConverter.convert(x)).filter(x -> x != null);

                resultDocMeta = publicationDocMeta.union(softwareDocMeta).union(orpDocMeta).union(datasetDocMeta);
    
            }
            resultDocMeta.cache();
            
            
            JavaRDD<eu.dnetlib.iis.importer.schemas.Project> resultProject = sourceProject.toJavaRDD()
                    .filter(x -> resultApprover.approve(x)).map(x -> projectConverter.convert(x)).filter(x -> x != null);
            resultProject.cache();

            JavaRDD<eu.dnetlib.iis.importer.schemas.Organization> resultOrganization = sourceOrganization.toJavaRDD()
                    .filter(x -> resultApprover.approve(x)).map(x -> organizationConverter.convert(x)).filter(x -> x != null);
            resultOrganization.cache();
            
            // handling relations
            // TODO possible optimization: each subsequent relation type could be run on rels.except(previouslyFilteredSet) with caching subsequent, reusable steps
            // TODO in previous importer we were applying resultApprover on entities only and retrieving relations from accepted rows (so we were running approver on body only) in this impl we run resultApprover on relations
            JavaRDD<eu.dnetlib.dhp.schema.oaf.Relation> sourceRelationRdd = sourceRelation.toJavaRDD();

            
            JavaRDD<DocumentToProject> docProjResultRelation = sourceRelationRdd
                    .filter(x -> acceptRelation(x, REL_TYPE_RESULT_PROJECT, SUBREL_TYPE_OUTCOME, REL_NAME_IS_PRODUCED_BY))
                    .filter(x -> resultApprover.approve(x)).map(x -> docProjectConverter.convert(x));
            docProjResultRelation.cache();
            
            JavaRDD<ProjectToOrganization> projOrgResultRelation = sourceRelationRdd
                    .filter(x -> acceptRelation(x, REL_TYPE_PROJECT_ORGANIZATION, SUBREL_TYPE_PARTICIPATION, REL_NAME_HAS_PARTICIPANT))
                    .filter(x -> resultApprover.approve(x)).map(x -> projectOrganizationConverter.convert(x));
            projOrgResultRelation.cache();

            
            JavaRDD<IdentifierMapping> dedupResultRelation = sourceRelationRdd
                    .filter(x -> acceptRelation(x, REL_TYPE_RESULT_RESULT, SUBREL_TYPE_DEDUP, REL_NAME_MERGES))
                    .filter(x -> resultApprover.approve(x)).map(x -> deduplicationMappingConverter.convert(x));
            dedupResultRelation.cache();
            
            storeInOutput(sc, resultDocMeta, resultDataset, resultProject, resultOrganization, docProjResultRelation, projOrgResultRelation, dedupResultRelation, params);
            
        } finally {
            // unless session is managed which is not our case
            if (Objects.nonNull(session)) {
                session.stop();
            }
        }
    }
    
    private static JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> parseToDocMeta(Dataset<? extends eu.dnetlib.dhp.schema.oaf.Result> source, 
            ResultApprover resultApprover, DocumentMetadataConverter documentConverter) {
        return source.toJavaRDD()
                .filter(x -> resultApprover.approve(x)).map(x -> documentConverter.convert(x))
                .filter(x -> x != null);
    }
    
    private static boolean acceptRelation(Relation relation, String relType, String subRelType, String relClass) {
        return relType.equals(relation.getRelType()) && subRelType.equals(relation.getSubRelType()) && relClass.equals(relation.getRelClass());    
    }
    
    /**
     * Creates data approver.
     */
    private static DataInfoBasedApprover buildApprover(String skipDeletedByInference, String trustLevelThreshold, String inferenceProvenanceBlacklist) {
        Float trustLevelThresholdFloat = null;
        if (StringUtils.isNotBlank(trustLevelThreshold) && !UNDEFINED_NONEMPTY_VALUE.equals(trustLevelThreshold)) {
            trustLevelThresholdFloat = Float.valueOf(trustLevelThreshold);
        }
        return new DataInfoBasedApprover(inferenceProvenanceBlacklist, Boolean.valueOf(skipDeletedByInference), trustLevelThresholdFloat);
    }
    
    private static <T extends Oaf> Dataset<T> readGraphTableFromSession(SparkSession spark, String inputGraphTablePath,
            Class<T> clazz) {
        return spark.read().textFile(inputGraphTablePath).map(
                //TODO add explicit dependency in a pom.xml: jackson
                // FIXME sync with przemek, reuse ObjectMapper and Encoder classes instead of instantiating it whenever possible
                (MapFunction<String, T>) value -> new ObjectMapper().readValue(value, clazz), Encoders.bean(clazz));
    }
    
    private static JavaRDD<ReportEntry> generateReportEntries(JavaSparkContext sparkContext, long docMetaCount,
            long datasetCount, long projectCount, long organizationCount, long docProjCount, long projOrgCount,
            long dedupDocCount) {
        return sparkContext.parallelize(Lists.newArrayList(
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_DOCMETADATA, docMetaCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_DATASET, datasetCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_PROJECT, projectCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_ORGANIZATION, organizationCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_DOC_PROJ_REFERENCE, docProjCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_PROJ_ORG_REFERENCE, projOrgCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_READ_DOC_DEDUP_DOC_REFERENCE, dedupDocCount)));
    }
    
    private static void storeInOutput(JavaSparkContext sparkContext,
            JavaRDD<eu.dnetlib.iis.importer.schemas.DocumentMetadata> docMeta,
            JavaRDD<eu.dnetlib.iis.importer.schemas.DataSetReference> dataset,
            JavaRDD<eu.dnetlib.iis.importer.schemas.Project> project,
            JavaRDD<eu.dnetlib.iis.importer.schemas.Organization> organization,
            JavaRDD<DocumentToProject> docProjResultRelation, JavaRDD<ProjectToOrganization> projOrgResultRelation,
            JavaRDD<IdentifierMapping> dedupResultRelation, ImportInformationSpaceJobParameters jobParams) {

        JavaRDD<ReportEntry> reports = generateReportEntries(sparkContext, docMeta.count(), dataset.count(),
                project.count(), organization.count(), docProjResultRelation.count(), projOrgResultRelation.count(),
                dedupResultRelation.count());

        avroSaver.saveJavaRDD(docMeta, eu.dnetlib.iis.importer.schemas.DocumentMetadata.SCHEMA$,
                jobParams.outputPath + '/' + jobParams.outputNameDocumentMeta);
        avroSaver.saveJavaRDD(dataset, eu.dnetlib.iis.importer.schemas.DataSetReference.SCHEMA$,
                jobParams.outputPath + '/' + jobParams.outputNameDatasetMeta);
        avroSaver.saveJavaRDD(project, eu.dnetlib.iis.importer.schemas.Project.SCHEMA$,
                jobParams.outputPath + '/' + jobParams.outputNameProject);
        avroSaver.saveJavaRDD(organization, eu.dnetlib.iis.importer.schemas.Organization.SCHEMA$,
                jobParams.outputPath + '/' + jobParams.outputNameOrganization);
        avroSaver.saveJavaRDD(docProjResultRelation, DocumentToProject.SCHEMA$,
                jobParams.outputPath + '/' + jobParams.outputNameDocumentProject);
        avroSaver.saveJavaRDD(projOrgResultRelation, ProjectToOrganization.SCHEMA$,
                jobParams.outputPath + '/' + jobParams.outputNameProjectOrganization);
        avroSaver.saveJavaRDD(dedupResultRelation, IdentifierMapping.SCHEMA$,
                jobParams.outputPath + '/' + jobParams.outputNameDedupMapping);

        avroSaver.saveJavaRDD(reports, ReportEntry.SCHEMA$, jobParams.outputReportPath);
    }
    
    private static final Class[] provideOafClasses() {
        // FIXME how to make this more flexible?
        return new Class[]{
                Author.class,
                Context.class,
                Country.class,
                DataInfo.class,
                eu.dnetlib.dhp.schema.oaf.Dataset.class,
                Datasource.class,
                ExternalReference.class,
                ExtraInfo.class,
                Field.class,
                GeoLocation.class,
                Instance.class,
                Journal.class,
                KeyValue.class,
                Oaf.class,
                OafEntity.class,
                OAIProvenance.class,
                Organization.class,
                OriginDescription.class,
                OtherResearchProduct.class,
                Project.class,
                Publication.class,
                Qualifier.class,
                Relation.class,
                Result.class,
                Software.class,
                StructuredProperty.class
        };
    }
    
    @Parameters(separators = "=")
    private static class ImportInformationSpaceJobParameters {

        @Parameter(names = "-skipDeletedByInference", required = true)
        private String skipDeletedByInference;
        
        @Parameter(names = "-trustLevelThreshold", required = true)
        private String trustLevelThreshold;
        
        @Parameter(names = "-inferenceProvenanceBlacklist", required = true)
        private String inferenceProvenanceBlacklist;
        
        @Parameter(names = "-inputRootPath", required = true)
        private String inputRootPath;

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
        
        @Parameter(names = "-outputNameDedupMapping", required = true)
        private String outputNameDedupMapping;
        
        @Parameter(names = "-outputNameOrganization", required = true)
        private String outputNameOrganization;
        
        @Parameter(names = "-outputNameProjectOrganization", required = true)
        private String outputNameProjectOrganization;
    }
    
}
