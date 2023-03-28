package eu.dnetlib.iis.wf.importer.infospace;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.HdfsTestUtils;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.importer.schemas.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author mhorst
 *
 */
public class ImportInformationSpaceJobTest extends TestWithSharedSparkSession {

    @TempDir
    public Path workingDir;

    private Path inputDir;
    private Path inputGraphDir;
    private Path outputDir;
    private Path outputReportDir;
    
    private static final String OUTPUT_NAME_DOCMETA = "docmeta";
    private static final String OUTPUT_NAME_DATASET = "dataset";
    private static final String OUTPUT_NAME_PROJECT = "project";
    private static final String OUTPUT_NAME_ORGANIZATION = "organization";
    private static final String OUTPUT_NAME_SERVICE = "service";
    private static final String OUTPUT_NAME_DOC_PROJ = "doc-proj";
    private static final String OUTPUT_NAME_PROJ_ORG = "proj-org";
    private static final String OUTPUT_NAME_IDENTIFIER = "identifier";
    
    @BeforeEach
    public void beforeEach() {
        super.beforeEach();

        inputDir = workingDir.resolve("input");
        inputGraphDir = inputDir.resolve("graph");

        outputDir = workingDir.resolve("output");
        outputReportDir = workingDir.resolve("output_report");
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
                eu.dnetlib.dhp.schema.oaf.Datasource.class,
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
                "-sharedSparkSession",
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
                "-outputNameIdentifierMapping", OUTPUT_NAME_IDENTIFIER,
                "-outputNameOrganization", OUTPUT_NAME_ORGANIZATION,
                "-outputNameService", OUTPUT_NAME_SERVICE,
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
        String expectedServicePath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/service.json");
        String expectedDocProjectPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/docproject.json");
        String expectedProjOrgPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/project_organization.json");
        String expectedIdentifierMappingPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/identifiermapping.json");
        String expectedReportPath = ClassPathResourceProvider.getResourcePath("eu/dnetlib/iis/wf/importer/infospace/output/report.json");
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_DOCMETA).toString(), expectedDocumentPath, DocumentMetadata.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_DATASET).toString(), expectedDatasetPath, DataSetReference.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_PROJECT).toString(), expectedProjectPath, Project.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_ORGANIZATION).toString(), expectedOrganizationPath, Organization.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_SERVICE).toString(), expectedServicePath, Service.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_DOC_PROJ).toString(), expectedDocProjectPath, DocumentToProject.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_PROJ_ORG).toString(), expectedProjOrgPath, ProjectToOrganization.class);
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(outputDir.resolve(OUTPUT_NAME_IDENTIFIER).toString(), expectedIdentifierMappingPath, IdentifierMapping.class);

        assertEquals(1,
                HdfsTestUtils.countFiles(new Configuration(), outputReportDir.toString(), DataStore.AVRO_FILE_EXT));
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
    
    private <T extends Oaf> Dataset<T> readGraphTableFromJSON(Path path, Class<T> clazz) {
        ObjectMapper objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return spark().read().format("json").load(path.toString()).toJSON()
                .map((MapFunction<String, T>) json -> objectMapper.readValue(json, clazz), Encoders.bean(clazz));
    }
    
}
