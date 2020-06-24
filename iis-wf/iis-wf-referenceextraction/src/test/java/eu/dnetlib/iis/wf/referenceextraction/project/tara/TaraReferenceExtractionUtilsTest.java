package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.common.spark.pipe.PipeExecutionEnvironment;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentHash;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentHashToProject;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.PublicationType;
import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static eu.dnetlib.iis.wf.referenceextraction.project.tara.TaraReferenceExtractionUtils.*;
import static org.apache.spark.sql.functions.lit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaraReferenceExtractionUtilsTest {

    private static SparkSession spark;

    @BeforeClass
    public static void beforeClass() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.set("spark.driver.host", "localhost");
        conf.setAppName(TaraReferenceExtractionUtilsTest.class.getSimpleName());
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    @AfterClass
    public static void afterClass() {
        spark.stop();
    }

    @Test
    public void buildDocumentMetadataShouldRunProperly() {
        // given
        AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark);
        Dataset<Row> documentTextDF = avroDataFrameSupport.createDataFrame(
                Arrays.asList(
                        createDocumentText("docId-1", "text-1"),
                        createDocumentText("docId-2", "text-2")
                ),
                DocumentText.SCHEMA$);
        Dataset<Row> extractedDocumentMetadataMergedWithOriginalDF = avroDataFrameSupport.createDataFrame(
                Arrays.asList(
                        createExtractedDocumentMetadataMergedWithOriginal("docId-1"),
                        createExtractedDocumentMetadataMergedWithOriginal("docId-a")
                ),
                ExtractedDocumentMetadataMergedWithOriginal.SCHEMA$);

        // when
        List<Row> results = buildDocumentMetadata(documentTextDF, extractedDocumentMetadataMergedWithOriginalDF)
                .collectAsList();

        // then
        List<Row> sortedResults = results.stream()
                .sorted(Comparator.comparing(o -> o.<String>getAs("id")))
                .collect(Collectors.toList());
        assertEquals(2, sortedResults.size());
        assertForDocumentMetadataRow(sortedResults.get(0), "docId-1", "text-1", null, null);
        assertForDocumentMetadataRow(sortedResults.get(1), "docId-2", "text-2", null, null);
    }

    @Test
    public void buildDocumentMetadataWithHashShouldRunProperly() {
        // given
        Dataset<Row> documentMetadataDF = spark.createDataFrame(
                Collections.singletonList(
                        createDocumentMetadata("id")
                ),
                CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA);
        DocumentMetadataHashColumnCreator hashColumnCreator = mock(DocumentMetadataHashColumnCreator.class);
        when(hashColumnCreator.hashCol("title", "abstract", "text"))
                .thenReturn(lit(1));

        // when
        List<Row> results = buildDocumentMetadataWithHash(documentMetadataDF, hashColumnCreator)
                .collectAsList();

        // then
        assertEquals(1, results.size());
        Row row = results.get(0);
        assertEquals("id", row.getAs("id"));
        assertEquals(1, row.<Integer>getAs("hashValue").intValue());
    }

    @Test
    public void documentMetadataToBeProcessedShouldFilterOutPreviouslyProcessedDocuments() {
        // given
        Dataset<Row> documentMetadataWithHashDF = spark.createDataFrame(
                Arrays.asList(
                        createDocumentMetadataWithHash("docId-1", 1),
                        createDocumentMetadataWithHash("docId-2", 2)
                ),
                CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_WITH_HASH_SCHEMA);
        Dataset<Row> documentHashFromCacheDF = new AvroDataFrameSupport(spark).createDataFrame(
                Arrays.asList(
                        createDocumentHash(2),
                        createDocumentHash(3)
                ),
                DocumentHash.SCHEMA$);

        // when
        List<Row> results = documentMetadataToBeProcessed(documentMetadataWithHashDF, documentHashFromCacheDF)
                .collectAsList();

        // then
        assertEquals(1, results.size());
        assertForDocumentMetadataRow(results.get(0), "docId-1", null, null, null);
    }

    @Test
    public void shouldRunReferenceExtraction() throws IOException {
        // given
        Dataset<Row> documentMetadataDF = spark.createDataFrame(
                Collections.singletonList(
                        createDocumentMetadata("id")
                ),
                CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA);
        PipeExecutionEnvironment pipeExecutionEnvironment = () -> {
            Path scriptWithInputCheck = createTestScriptWithInputCheck();
            spark.sparkContext().addFile(scriptWithInputCheck.toString());
            return String.format("bash %s/%s", SparkFiles.getRootDirectory(),
                    scriptWithInputCheck.getFileName().toString());
        };

        // when
        List<Row> results = runReferenceExtraction(spark, documentMetadataDF, pipeExecutionEnvironment)
                .collectAsList();

        // then
        assertEquals(1, results.size());
        Row row = results.get(0);
        assertEquals("docId-1", row.getAs("documentId"));
        assertEquals("projId-1", row.getAs("projectId"));
        assertEquals(1.0f, row.<Float>getAs("confidenceLevel"), 1e-3);
        assertNull(row.getAs("textsnippet"));
    }

    private static Path createTestScriptWithInputCheck() throws IOException {
        String content = String.join(System.getProperty("line.separator"),
                "#!/bin/bash",
                "read in",
                "test ${in:0:1} == '{' -a ${in: -1} == '}' && echo '{\"documentId\":\"docId-1\",\"projectId\":\"projId-1\",\"confidenceLevel\":1,\"textsnippet\":null}'"
        );
        return Files.write(Files.createTempFile(null, "sh"), content.getBytes());
    }

    @Test
    public void documentHashToProjectToBeCachedShouldRunProperly() {
        // given
        AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark);
        Dataset<Row> documentToProjectDF = avroDataFrameSupport.createDataFrame(
                Collections.singletonList(
                        createDocumentToProject("docId-1", "projId-1", 1f)
                ),
                DocumentToProject.SCHEMA$);
        Dataset<Row> documentHashToProjectFromCacheDF = new AvroDataFrameSupport(spark).createDataFrame(
                Collections.singletonList(
                        createDocumentHashToProject(2, "projId-2", 2f)
                ),
                DocumentHashToProject.SCHEMA$);
        Dataset<Row> documentMetadataWithHashDF = spark.createDataFrame(
                Arrays.asList(
                        createDocumentMetadataWithHash("docId-1", 1),
                        createDocumentMetadataWithHash("docId-2", 2)
                ),
                CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_WITH_HASH_SCHEMA);

        // when
        Dataset<Row> resultDF = documentHashToProjectToBeCached(spark,
                documentToProjectDF,
                documentHashToProjectFromCacheDF,
                documentMetadataWithHashDF);

        // then
        assertAvroSchemaEqualsSqlSchema(DocumentHashToProject.SCHEMA$, resultDF.schema());
        List<Row> results = resultDF.collectAsList();
        assertEquals(2, results.size());
        List<Row> resultsSorted = results.stream()
                .sorted(Comparator.comparing(o -> o.<Integer>getAs("hashValue")))
                .collect(Collectors.toList());
        assertForDocumentHashToProject(resultsSorted.get(0), 1, "projId-1", 1f);
        assertForDocumentHashToProject(resultsSorted.get(1), 2, "projId-2", 2f);
    }

    @Test
    public void documentHashToBeCachedShouldRunProperly() {
        // given
        Dataset<Row> documentHashFromCacheDF = new AvroDataFrameSupport(spark).createDataFrame(
                Arrays.asList(
                        createDocumentHash(2),
                        createDocumentHash(3)
                ),
                DocumentHash.SCHEMA$);
        Dataset<Row> documentMetadataWithHashDF = spark.createDataFrame(
                Arrays.asList(
                        createDocumentMetadataWithHash("docId-1", 1),
                        createDocumentMetadataWithHash("docId-2", 2)
                ),
                CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_WITH_HASH_SCHEMA);

        // when
        Dataset<Row> resultDF = documentHashToBeCached(spark,
                documentHashFromCacheDF,
                documentMetadataWithHashDF);

        // then
        assertAvroSchemaEqualsSqlSchema(DocumentHash.SCHEMA$, resultDF.schema());
        List<Row> results = resultDF.collectAsList();
        assertEquals(3, results.size());
        List<Object> hashValues = results.stream()
                .map(row -> row.<Integer>getAs("hashValue"))
                .sorted()
                .collect(Collectors.toList());
        assertEquals(1, hashValues.get(0));
        assertEquals(2, hashValues.get(1));
        assertEquals(3, hashValues.get(2));
    }

    @Test
    public void documentToProjectToOutputShouldRunProperly() {
        // given
        Dataset<Row> documentHashToProjectDF = new AvroDataFrameSupport(spark).createDataFrame(
                Collections.singletonList(
                        createDocumentHashToProject(1, "projId-1", 1f)
                ),
                DocumentHashToProject.SCHEMA$);
        Dataset<Row> documentMetadataWithHashDF = spark.createDataFrame(
                Arrays.asList(
                        createDocumentMetadataWithHash("docId-1", 1),
                        createDocumentMetadataWithHash("docId-2", 2)
                ),
                CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_WITH_HASH_SCHEMA);

        // when
        Dataset<Row> resultDF = documentToProjectToOutput(spark,
                documentHashToProjectDF,
                documentMetadataWithHashDF);

        // then
        assertAvroSchemaEqualsSqlSchema(DocumentToProject.SCHEMA$, resultDF.schema());
        List<Row> results = resultDF.collectAsList();
        assertEquals(1, results.size());
        Row row = results.get(0);
        assertEquals("docId-1", row.getAs("documentId"));
        assertEquals("projId-1", row.getAs("projectId"));
        assertEquals(1f, row.<Float>getAs("confidenceLevel"), 1e-3);
        assertNull(row.getAs("textsnippet"));
    }

    private static DocumentText createDocumentText(String id, String text) {
        return DocumentText.newBuilder()
                .setId(id)
                .setText(text)
                .build();
    }

    private static ExtractedDocumentMetadataMergedWithOriginal createExtractedDocumentMetadataMergedWithOriginal(String id) {
        return ExtractedDocumentMetadataMergedWithOriginal.newBuilder()
                .setId(id)
                .setPublicationType(PublicationType.newBuilder().build())
                .build();
    }

    private static void assertForDocumentMetadataRow(Row row,
                                                     String id,
                                                     String text,
                                                     String title,
                                                     String abstract$) {
        assertEquals(id, row.getAs("id"));
        assertEquals(text, row.getAs("text"));
        assertEquals(title, row.getAs("title"));
        assertEquals(abstract$, row.getAs("abstract"));
    }

    private static Row createDocumentMetadata(String id) {
        return RowFactory.create(id, null, null, null);
    }

    private static Row createDocumentMetadataWithHash(String id,
                                                      Integer hashValue) {
        return RowFactory.create(id, null, null, null, hashValue);
    }

    private static DocumentHash createDocumentHash(Integer hashValue) {
        return DocumentHash.newBuilder().setHashValue(hashValue).build();
    }

    private static DocumentToProject createDocumentToProject(String documentId,
                                                             String projectId,
                                                             Float confidenceLevel) {
        return DocumentToProject.newBuilder()
                .setDocumentId(documentId)
                .setProjectId(projectId)
                .setConfidenceLevel(confidenceLevel)
                .build();
    }

    private static DocumentHashToProject createDocumentHashToProject(Integer hashValue,
                                                                     String projectId,
                                                                     Float confidenceLevel) {
        return DocumentHashToProject.newBuilder()
                .setHashValue(hashValue)
                .setProjectId(projectId)
                .setConfidenceLevel(confidenceLevel)
                .build();
    }

    private static void assertForDocumentHashToProject(Row row,
                                                       Integer hashValue,
                                                       String projectId,
                                                       Float confidenceLevel) {
        assertEquals(hashValue, row.getAs("hashValue"));
        assertEquals(projectId, row.getAs("projectId"));
        assertEquals(confidenceLevel, row.<Float>getAs("confidenceLevel"), 1e-3);
        assertNull(row.getAs("textsnippet"));
    }

    private static void assertAvroSchemaEqualsSqlSchema(Schema avroSchema,
                                                        StructType sqlSchema) {
        assertEquals(SchemaConverters.toSqlType(avroSchema).dataType(), sqlSchema);
    }
}