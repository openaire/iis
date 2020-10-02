package eu.dnetlib.iis.wf.importer.infospace;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.IntegrationTest;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.importer.schemas.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author mhorst
 *
 */
@IntegrationTest
public class ImportInformationSpaceJobTest {

    private static SparkSession spark;

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
    
    @BeforeAll
    public static void beforeAll() {
        SparkConf conf = new SparkConf();
        conf.setAppName(ImportInformationSpaceJobTest.class.getSimpleName());
        conf.setMaster("local");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.registerKryoClasses(OafModelUtils.provideOafClasses());

        spark = SparkSession.builder().config(conf).getOrCreate();

    }

    @BeforeEach
    public void before() throws IOException {
        workingDir = Files.createTempDirectory("test_import_info_space");
        inputDir = workingDir.resolve("input");
        inputGraphDir = inputDir.resolve("graph");

        outputDir = workingDir.resolve("output");
        outputReportDir = workingDir.resolve("output_report");
    }

    @AfterEach
    public void after() throws IOException {
        FileUtils.deleteDirectory(workingDir.toFile());
    }

    @AfterAll
    public static void afterAll() {
        spark.stop();
    }
    
    @Test
    public void testImportFromTextGraph() throws Exception {
        testImportFromGraph("json");
    }
    
    @Test
    public void testImportFromParquetGraph() throws Exception {
        testImportFromGraph("parquet");
    }
    
    private void testImportFromGraph(String format) throws Exception {
        
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
            createGraphTableFor(inputGraphTableJsonDumpPath, graphTableName, graphClass, format);
        }
        
        // when
        ImportInformationSpaceJob.main(new String[]{
                "-sparkSessionManagedOutside",
                "-skipDeletedByInference", Boolean.TRUE.toString(),
                "-trustLevelThreshold", "0.7",
                "-inferenceProvenanceBlacklist", "iis",
                "-inputFormat", format,
                "-inputRootPath", inputGraphDir.toString(),
                "-outputPath", outputDir.toString(),
                "-outputReportPath", outputReportDir.toString(),
                "-outputNameDocumentMeta", OUTPUT_NAME_DOCMETA,
                "-outputNameDatasetMeta", OUTPUT_NAME_DATASET,
                "-outputNameDocumentProject", OUTPUT_NAME_DOC_PROJ,
                "-outputNameProject", OUTPUT_NAME_PROJECT,
                "-outputNameDedupMapping", OUTPUT_NAME_DEDUP,
                "-outputNameOrganization", OUTPUT_NAME_ORGANIZATION,
                "-outputNameProjectOrganization", OUTPUT_NAME_PROJ_ORG,
                "-maxDescriptionLength", "1000",
                "-maxTitlesSize", "1",
                "-maxTitleLength", "75",
                "-maxAuthorsSize", "25",
                "-maxAuthorFullnameLength", "25",
                "-maxKeywordsSize", "5",
                "-maxKeywordLength", "15"
        });
        
        // then
        String expectedDocumentPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/document.json");
        String expectedDatasetPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/dataset.json");
        String expectedProjectPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/project.json");
        String expectedOrganizationPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/organization.json");
        String expectedDocProjectPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/docproject.json");
        String expectedProjOrgPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/project_organization.json");
        String expectedDedupMappingPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/dedupmapping.json");
        String expectedReportPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/report.json");
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_DOCMETA).toString(), expectedDocumentPath, DocumentMetadata.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_DATASET).toString(), expectedDatasetPath, DataSetReference.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_PROJECT).toString(), expectedProjectPath, Project.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_ORGANIZATION).toString(), expectedOrganizationPath, Organization.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_DOC_PROJ).toString(), expectedDocProjectPath, DocumentToProject.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_PROJ_ORG).toString(), expectedProjOrgPath, ProjectToOrganization.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_DEDUP).toString(), expectedDedupMappingPath, IdentifierMapping.class);

        assertEquals(1,
                HdfsUtils.countFiles(new Configuration(), outputReportDir.toString(), x -> x.getName().endsWith(DataStore.AVRO_FILE_EXT)));
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputReportDir.toString(), expectedReportPath, ReportEntry.class);
    }
    
    
    private <T extends Oaf> void createGraphTableFor(String inputGraphTableJsonDumpPath,
            String inputGraphTableDirRelativePath, Class<T> clazz, String format) {
        Path inputGraphTableJsonDumpFile = Paths.get(
                ClassPathResourceProvider.getResourcePath(inputGraphTableJsonDumpPath));

        Dataset<T> inputGraphTableDS = readGraphTableFromJSON(inputGraphTableJsonDumpFile, clazz);
        Path inputGraphTableDir = inputGraphDir.resolve(inputGraphTableDirRelativePath);
        
        inputGraphTableDS.write().format(format).save(inputGraphTableDir.toString());
    }
    
    private static <T extends Oaf> Dataset<T> readGraphTableFromJSON(Path path, Class<T> clazz) {
        ObjectMapper objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return spark.read().format("json").load(path.toString()).toJSON()
                .map((MapFunction<String, T>) json -> objectMapper.readValue(json, clazz), Encoders.bean(clazz));
    }
    
}
