package eu.dnetlib.iis.wf.importer.infospace;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.importer.schemas.DataSetReference;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import eu.dnetlib.iis.importer.schemas.Organization;
import eu.dnetlib.iis.importer.schemas.Project;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;

/**
 * @author mhorst
 *
 */
@Category(IntegrationTest.class)
public class ImportInformationSpaceJobTest {

    
    private static SparkSession spark;
    private static Configuration configuration;

    private Path workingDir;
    private Path inputDir;
    private Path inputGraphDir;
    private Path outputDir;
    private Path outputReportDir;
    
    private static final String OUTPUT_NAME_DOCMETA = "docmeta";
    private static final String OUTPUT_NAME_DATASET = "dataset";
    private static final String OUTPUT_NAME_PROJECT = "project";
    private static final String OUTPUT_NAME_ORGANIZATION = "organzation";
    private static final String OUTPUT_NAME_DOC_PROJ = "doc-proj";
    private static final String OUTPUT_NAME_PROJ_ORG = "proj-org";
    private static final String OUTPUT_NAME_DEDUP = "dedup";
    

    private ClassLoader cl = getClass().getClassLoader();

    @BeforeClass
    public static void beforeClass() throws IOException {
        SparkConf conf = new SparkConf();
        conf.setAppName(ImportInformationSpaceJobTest.class.getSimpleName());
        conf.setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(OafModelUtils.provideOafClasses());

        spark = SparkSession.builder().config(conf).getOrCreate();

        configuration = Job.getInstance().getConfiguration();
    }

    @Before
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("test_import_info_space");
        inputDir = workingDir.resolve("input");
        inputGraphDir = inputDir.resolve("graph");

        outputDir = workingDir.resolve("output");
        outputReportDir = workingDir.resolve("output_report");
    }

    @After
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @AfterClass
    public static void afterAll() {
        spark.stop();
    }
    
    @Test
    public void testImportFromGraph() throws Exception {
        
        @SuppressWarnings("unchecked")
        Class<? extends Oaf> graphClasses[] = new Class[] {
                eu.dnetlib.dhp.schema.oaf.Dataset.class,
                eu.dnetlib.dhp.schema.oaf.Organization.class,
                eu.dnetlib.dhp.schema.oaf.OtherResearchProduct.class,
                eu.dnetlib.dhp.schema.oaf.Software.class,
                eu.dnetlib.dhp.schema.oaf.Publication.class,
                eu.dnetlib.dhp.schema.oaf.Project.class,
                eu.dnetlib.dhp.schema.oaf.Relation.class
        };
        
        for (Class<? extends Oaf> graphClass : graphClasses) {
            String graphTableName = graphClass.getSimpleName().toLowerCase();
            String inputGraphTableJsonDumpPath = String.format("%s/%s.json",
                    "eu/dnetlib/iis/wf/importer/infospace/input/graph", graphTableName);
            createGraphTableFor(inputGraphTableJsonDumpPath, graphTableName, graphClass);
        }
        
        // when
        ImportInformationSpaceJob.main(new String[]{
                "-skipDeletedByInference", Boolean.TRUE.toString(),
                "-trustLevelThreshold", "0.7",
                "-inferenceProvenanceBlacklist", "iis",
                "-inputRootPath", inputGraphDir.toString(),
                "-outputPath", outputDir.toString(),
                "-outputReportPath", outputReportDir.toString(),
                "-outputNameDocumentMeta", OUTPUT_NAME_DOCMETA,
                "-outputNameDatasetMeta", OUTPUT_NAME_DATASET,
                "-outputNameDocumentProject", OUTPUT_NAME_DOC_PROJ,
                "-outputNameProject", OUTPUT_NAME_PROJECT,
                "-outputNameDedupMapping", OUTPUT_NAME_DEDUP,
                "-outputNameOrganization", OUTPUT_NAME_ORGANIZATION,
                "-outputNameProjectOrganization", OUTPUT_NAME_PROJ_ORG
        });
        
        // then
        String expectedDocumentPath = Objects.requireNonNull(cl.getResource("eu/dnetlib/iis/wf/importer/infospace/output/document.json")).getFile();
        String expectedDatasetPath = Objects.requireNonNull(cl.getResource("eu/dnetlib/iis/wf/importer/infospace/output/dataset.json")).getFile();
        String expectedProjectPath = Objects.requireNonNull(cl.getResource("eu/dnetlib/iis/wf/importer/infospace/output/project.json")).getFile();
        String expectedOrganizationPath = Objects.requireNonNull(cl.getResource("eu/dnetlib/iis/wf/importer/infospace/output/organization.json")).getFile();
        String expectedDocProjectPath = Objects.requireNonNull(cl.getResource("eu/dnetlib/iis/wf/importer/infospace/output/docproject.json")).getFile();
        String expectedProjOrgPath = Objects.requireNonNull(cl.getResource("eu/dnetlib/iis/wf/importer/infospace/output/project_organization.json")).getFile();
        String expectedDedupMappingPath = Objects.requireNonNull(cl.getResource("eu/dnetlib/iis/wf/importer/infospace/output/dedupmapping.json")).getFile();
        String expectedReportPath = Objects.requireNonNull(cl.getResource("eu/dnetlib/iis/wf/importer/infospace/output/report.json")).getFile();
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_DOCMETA).toString(), expectedDocumentPath, DocumentMetadata.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_DATASET).toString(), expectedDatasetPath, DataSetReference.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_PROJECT).toString(), expectedProjectPath, Project.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_ORGANIZATION).toString(), expectedOrganizationPath, Organization.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_DOC_PROJ).toString(), expectedDocProjectPath, DocumentToProject.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_PROJ_ORG).toString(), expectedProjOrgPath, ProjectToOrganization.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_DEDUP).toString(), expectedDedupMappingPath, IdentifierMapping.class);
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), expectedReportPath, ReportEntry.class);
    }
    
    
    private <T extends Oaf> void createGraphTableFor(String inputGraphTableJsonDumpPath,
            String inputGraphTableDirRelativePath, Class<T> clazz) {
        Path inputGraphTableJsonDumpFile = Paths
                .get(Objects.requireNonNull(cl.getResource(inputGraphTableJsonDumpPath)).getFile());
        Dataset<T> inputGraphTableDS = readGraphTableFromJSON(inputGraphTableJsonDumpFile, clazz);
        Path inputGraphTableDir = inputGraphDir.resolve(inputGraphTableDirRelativePath);
        inputGraphTableDS.toJSON().javaRDD()
        // writing as sequence file
//                .mapToPair(json -> new Tuple2<>(new Text(clazz.getCanonicalName()), new Text(json)))
//                .saveAsNewAPIHadoopFile(inputGraphTableDir.toString(), Text.class, Text.class,
//                        SequenceFileOutputFormat.class, configuration);
        // writing as plaintext file
                  .saveAsTextFile(inputGraphTableDir.toString());
    }
    
    private static <T extends Oaf> Dataset<T> readGraphTableFromJSON(Path path, Class<T> clazz) {
        ObjectMapper objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return spark.read().format("json").load(path.toString()).toJSON()
                .map((MapFunction<String, T>) json -> objectMapper.readValue(json, clazz), Encoders.bean(clazz));
    }
    
    private static <T extends Oaf> Dataset<T> readGraphTableFromParquet(String outputGraphTablePath, Class<T> clazz) {
        return spark.read().format("parquet").load(outputGraphTablePath).as(Encoders.bean(clazz));
    }
    
}
