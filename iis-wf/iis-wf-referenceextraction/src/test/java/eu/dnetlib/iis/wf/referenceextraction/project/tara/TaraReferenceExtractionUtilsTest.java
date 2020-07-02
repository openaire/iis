package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.common.spark.pipe.PipeExecutionEnvironment;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentHash;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentHashToProject;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.PublicationType;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkFiles;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
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
    public void buildDocumentMetadataByIdShouldRunProperly() {
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
        Dataset<Row> resultDF = buildDocumentMetadataById(documentTextDF, extractedDocumentMetadataMergedWithOriginalDF);

        // then
        assertEquals(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_BY_ID_SCHEMA, resultDF.schema());

        List<Row> results = resultDF.collectAsList().stream()
                .sorted(Comparator.comparing(o -> o.getAs("id")))
                .collect(Collectors.toList());
        assertEquals(2, results.size());
        assertForDocumentMetadataByIdRow(results.get(0), "docId-1", null, null, "text-1");
        assertForDocumentMetadataByIdRow(results.get(1), "docId-2", null, null, "text-2");
    }

    @Test
    public void buildDocumentMetadataShouldRunProperly() {
        // given
        Dataset<Row> documentMetadataByIdDF = spark.createDataFrame(
                Collections.singletonList(
                        createDocumentMetadataById("docId-1", "text-1")
                ),
                CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA);
        DocumentMetadataHashColumnCreator hashColumnCreator = mock(DocumentMetadataHashColumnCreator.class);
        when(hashColumnCreator.hashCol("title", "abstract", "text"))
                .thenReturn(lit("a1"));

        // when
        Dataset<Row> resultDF = buildDocumentMetadata(documentMetadataByIdDF, hashColumnCreator);

        // then
        assertEquals(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA, resultDF.schema());

        List<Row> results = resultDF.collectAsList();
        assertEquals(1, results.size());
        Row row = results.get(0);
        assertEquals("docId-1", row.getAs("id"));
        assertEquals("a1", row.getAs("hashValue"));
    }

    @Test
    public void documentMetadataByHashToBeProcessedShouldFilterOutPreviouslyProcessedDocuments() {
        // given
        Dataset<Row> documentMetadataDF = spark.createDataFrame(
                Arrays.asList(
                        createDocumentMetadata("docId-1", "text-1", "a1"),
                        createDocumentMetadata("docId-1", "text-1", "a1"),
                        createDocumentMetadata("docId-2", "text-2", "b2"),
                        createDocumentMetadata("docId-2", "text-2", "b2")
                ),
                CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA);
        Dataset<Row> documentHashFromCacheDF = new AvroDataFrameSupport(spark).createDataFrame(
                Arrays.asList(
                        createDocumentHash("b2"),
                        createDocumentHash("c3")
                ),
                DocumentHash.SCHEMA$);

        // when
        Dataset<Row> resultDF = documentMetadataByHashToBeProcessed(documentMetadataDF, documentHashFromCacheDF);

        // then
        assertEquals(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_BY_HASH_SCHEMA, resultDF.schema());

        List<Row> results = resultDF.collectAsList();
        assertEquals(1, results.size());
        assertForDocumentMetadataByHashRow(results.get(0), "a1", null, null, "text-1");
    }

    @Test
    public void shouldRunReferenceExtraction() throws IOException {
        // given
        Dataset<Row> documentMetadataByHashDF = spark.createDataFrame(
                Collections.singletonList(
                        createDocumentMetadataByHash("a1", "text-1")
                ),
                CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_BY_HASH_SCHEMA);
        PipeExecutionEnvironment pipeExecutionEnvironment = () -> {
            Path scriptWithInputCheck = createTestScriptWithInputCheck();
            spark.sparkContext().addFile(scriptWithInputCheck.toString());
            return String.format("bash %s/%s", SparkFiles.getRootDirectory(),
                    scriptWithInputCheck.getFileName().toString());
        };

        // when
        Dataset<Row> resultDF = runReferenceExtraction(spark, documentMetadataByHashDF, pipeExecutionEnvironment);

        // then
        assertEquals(SchemaConverters.toSqlType(DocumentHashToProject.SCHEMA$).dataType().asNullable(),
                resultDF.schema().asNullable());

        List<Row> results = resultDF.collectAsList();
        assertEquals(1, results.size());
        Row row = results.get(0);
        assertForDocumentHashToProject(row, "a1", "projId-1", 1.0f);
    }

    @Test
    public void documentHashToProjectToBeCachedShouldRunProperly() {
        // given
        AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark);
        Dataset<Row> documentHashToProjectDF = avroDataFrameSupport.createDataFrame(
                Collections.singletonList(
                        createDocumentHashToProject("a1", "projId-1", 1f)
                ),
                DocumentHashToProject.SCHEMA$);
        Dataset<Row> documentHashToProjectFromCacheDF = new AvroDataFrameSupport(spark).createDataFrame(
                Collections.singletonList(
                        createDocumentHashToProject("b2", "projId-2", 2f)
                ),
                DocumentHashToProject.SCHEMA$);

        // when
        Dataset<Row> resultDF = documentHashToProjectToBeCached(spark,
                documentHashToProjectDF,
                documentHashToProjectFromCacheDF);

        // then
        assertEquals(SchemaConverters.toSqlType(DocumentHashToProject.SCHEMA$).dataType(), resultDF.schema());

        List<Row> results = resultDF.collectAsList().stream()
                .sorted(Comparator.comparing(o -> o.getAs("hashValue")))
                .collect(Collectors.toList());
        assertEquals(2, results.size());

        assertForDocumentHashToProject(results.get(0), "a1", "projId-1", 1f);
        assertForDocumentHashToProject(results.get(1), "b2", "projId-2", 2f);
    }

    @Test
    public void documentHashToBeCachedShouldRunProperly() {
        // given
        Dataset<Row> documentHashFromCacheDF = new AvroDataFrameSupport(spark).createDataFrame(
                Arrays.asList(
                        createDocumentHash("b2"),
                        createDocumentHash("c3")
                ),
                DocumentHash.SCHEMA$);
        Dataset<Row> documentMetadataDF = spark.createDataFrame(
                Arrays.asList(
                        createDocumentMetadata("docId-1", "text-1", "a1"),
                        createDocumentMetadata("docId-2", "text-2", "b2")
                ),
                CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA);

        // when
        Dataset<Row> resultDF = documentHashToBeCached(spark,
                documentHashFromCacheDF,
                documentMetadataDF);

        // then
        assertEquals(SchemaConverters.toSqlType(DocumentHash.SCHEMA$).dataType(), resultDF.schema());

        List<Row> results = resultDF.collectAsList().stream()
                .sorted(Comparator.comparing(o -> o.getAs("hashValue")))
                .collect(Collectors.toList());
        assertEquals(3, results.size());

        assertEquals("a1", results.get(0).getAs("hashValue"));
        assertEquals("b2", results.get(1).getAs("hashValue"));
        assertEquals("c3", results.get(2).getAs("hashValue"));
    }

    @Test
    public void documentToProjectToOutputShouldRunProperly() {
        // given
        Dataset<Row> documentHashToProjectDF = new AvroDataFrameSupport(spark).createDataFrame(
                Collections.singletonList(
                        createDocumentHashToProject("a1", "projId-1", 1f)
                ),
                DocumentHashToProject.SCHEMA$);
        Dataset<Row> documentMetadataDF = spark.createDataFrame(
                Arrays.asList(
                        createDocumentMetadata("docId-1", "text-1", "a1"),
                        createDocumentMetadata("docId-2", "text-2", "b2")
                ),
                CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA);

        // when
        Dataset<Row> resultDF = documentToProjectToOutput(spark,
                documentHashToProjectDF,
                documentMetadataDF);

        // then
        assertEquals(SchemaConverters.toSqlType(DocumentToProject.SCHEMA$).dataType(), resultDF.schema());

        List<Row> results = resultDF.collectAsList().stream()
                .sorted(Comparator.comparing(o -> o.getAs("documentId")))
                .collect(Collectors.toList());
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

    private static Row createDocumentMetadataById(String id,
                                                  String text) {
        return RowFactory.create(id, null, null, text);
    }

    private static Row createDocumentMetadataByHash(String hashCode,
                                                    String text) {
        return RowFactory.create(hashCode, null, null, text);
    }

    private static Row createDocumentMetadata(String id,
                                              String text,
                                              String hashValue) {
        return RowFactory.create(id, null, null, text, hashValue);
    }

    private static DocumentHash createDocumentHash(String hashValue) {
        return DocumentHash.newBuilder().setHashValue(hashValue).build();
    }

    private static DocumentHashToProject createDocumentHashToProject(String hashValue,
                                                                     String projectId,
                                                                     Float confidenceLevel) {
        return DocumentHashToProject.newBuilder()
                .setHashValue(hashValue)
                .setProjectId(projectId)
                .setConfidenceLevel(confidenceLevel)
                .build();
    }

    private static Path createTestScriptWithInputCheck() throws IOException {
        String content = String.join(System.getProperty("line.separator"),
                "#!/bin/bash",
                "read in",
                "test ${in:0:1} == '{' -a ${in: -1} == '}' && echo '{\"documentId\":\"a1\",\"projectId\":\"projId-1\",\"confidenceLevel\":1,\"textsnippet\":null}'"
        );
        return Files.write(Files.createTempFile(null, "sh"), content.getBytes());
    }

    private static void assertForDocumentMetadataByIdRow(Row row,
                                                         String id,
                                                         String title,
                                                         String abstract$,
                                                         String text) {
        assertEquals(id, row.getAs("id"));
        assertEquals(title, row.getAs("title"));
        assertEquals(abstract$, row.getAs("abstract"));
        assertEquals(text, row.getAs("text"));
    }

    private static void assertForDocumentMetadataByHashRow(Row row,
                                                           String hashValue,
                                                           String title,
                                                           String abstract$,
                                                           String text) {
        assertEquals(hashValue, row.getAs("hashValue"));
        assertEquals(title, row.getAs("title"));
        assertEquals(abstract$, row.getAs("abstract"));
        assertEquals(text, row.getAs("text"));
    }

    private static void assertForDocumentHashToProject(Row row,
                                                       String hashValue,
                                                       String projectId,
                                                       Float confidenceLevel) {
        assertEquals(hashValue, row.getAs("hashValue"));
        assertEquals(projectId, row.getAs("projectId"));
        assertEquals(confidenceLevel, row.<Float>getAs("confidenceLevel"), 1e-3);
        assertNull(row.getAs("textsnippet"));
    }
}