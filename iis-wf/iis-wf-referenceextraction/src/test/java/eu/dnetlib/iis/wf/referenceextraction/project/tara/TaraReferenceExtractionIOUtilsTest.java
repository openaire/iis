package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import eu.dnetlib.iis.common.cache.CacheMetadataManagingProcess;
import eu.dnetlib.iis.common.lock.LockManager;
import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentHash;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentHashToProject;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static eu.dnetlib.iis.wf.referenceextraction.project.tara.TaraReferenceExtractionIOUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TaraReferenceExtractionIOUtilsTest {

    private static SparkSession spark;

    @BeforeClass
    public static void beforeClass() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.set("spark.driver.host", "localhost");
        conf.setAppName(TaraReferenceExtractionIOUtilsTest.class.getSimpleName());
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    @AfterClass
    public static void afterClass() {
        spark.stop();
    }

    @Test
    public void clearOutputShouldRunProperly() throws IOException {
        // given
        OutputCleaner cleaner = mock(OutputCleaner.class);

        // when
        clearOutput("path/to/output", cleaner);

        // then
        verify(cleaner, atLeastOnce()).clearOutput("path/to/output");
    }

    @Test
    public void readDocumentHashToProjectFromCacheOrEmptyShouldReadEmptyDataFrameIfExistingCacheIdIsUndefined() {
        // given
        AvroDataStoreReader reader = mock(AvroDataStoreReader.class);

        // when
        Dataset<Row> resultDF = readDocumentHashToProjectFromCacheOrEmpty(spark,
                "path/to/cache",
                CacheMetadataManagingProcess.UNDEFINED,
                reader);

        // then
        assertEquals(SchemaConverters.toSqlType(DocumentHashToProject.SCHEMA$).dataType(), resultDF.schema());

        List<Row> results = resultDF.collectAsList();
        assertTrue(results.isEmpty());

        verify(reader, never()).read(any(), any());
    }

    @Test
    public void readDocumentHashToProjectFromCacheOrEmptyShouldReadNonEmptyDataFraneIfExistingCacheIdIsDefined() {
        // given
        DocumentHashToProject documentHashToProject = DocumentHashToProject.newBuilder()
                .setHashValue("a1")
                .setProjectId("projectId")
                .setConfidenceLevel(1.0f)
                .build();
        List<DocumentHashToProject> documentHashToProjectList = Collections.singletonList(documentHashToProject);
        AvroDataStoreReader reader = mock(AvroDataStoreReader.class);
        when(reader.read(new Path("path/to/cache/01",
                        CachedTaraReferenceExtractionJob.CacheRecordType.documentHashToProject.name()).toString(),
                DocumentHashToProject.SCHEMA$))
                .thenReturn(new AvroDataFrameSupport(spark).createDataFrame(documentHashToProjectList,
                        DocumentHashToProject.SCHEMA$));

        // when
        Dataset<Row> resultDF = readDocumentHashToProjectFromCacheOrEmpty(spark,
                "path/to/cache",
                "01",
                reader);

        // then
        assertEquals(SchemaConverters.toSqlType(DocumentHashToProject.SCHEMA$).dataType(), resultDF.schema());

        List<Row> results = resultDF.collectAsList();
        assertEquals(1, results.size());
        Row row = results.get(0);
        assertEquals(documentHashToProject.getHashValue(), row.getAs("hashValue"));
        assertEquals(documentHashToProject.getProjectId(), row.getAs("projectId"));
        assertEquals(documentHashToProject.getConfidenceLevel(), row.<Float>getAs("confidenceLevel"), 1e-3);
        assertNull(row.getAs("textsnippet"));
    }

    @Test
    public void readDocumentHashFromCacheOrEmptyShouldReadEmptyDataFrameIfExistingCacheIdIsUndefined() {
        // given
        AvroDataStoreReader reader = mock(AvroDataStoreReader.class);

        // when
        Dataset<Row> resultDF = readDocumentHashFromCacheOrEmpty(spark,
                "path/to/cache",
                CacheMetadataManagingProcess.UNDEFINED,
                reader);

        // then
        assertEquals(SchemaConverters.toSqlType(DocumentHash.SCHEMA$).dataType(), resultDF.schema());

        List<Row> results = resultDF.collectAsList();
        assertTrue(results.isEmpty());

        verify(reader, never()).read(any(), any());
    }

    @Test
    public void readDocumentHashFromCacheOrEmptyShouldReadNonEmptyDataFrameIfExistingCacheIdIsDefined() {
        // given
        DocumentHash documentHash = DocumentHash.newBuilder().setHashValue("a1").build();
        List<DocumentHash> documentHashList = Collections.singletonList(documentHash);
        AvroDataStoreReader reader = mock(AvroDataStoreReader.class);
        when(reader.read(new Path("path/to/cache/01", CachedTaraReferenceExtractionJob.CacheRecordType.documentHash.name()).toString(),
                DocumentHash.SCHEMA$))
                .thenReturn(new AvroDataFrameSupport(spark).createDataFrame(documentHashList, DocumentHash.SCHEMA$));

        // when
        Dataset<Row> resultDF = readDocumentHashFromCacheOrEmpty(spark,
                "path/to/cache",
                "01",
                reader);

        // then
        assertEquals(SchemaConverters.toSqlType(DocumentHash.SCHEMA$).dataType(), resultDF.schema());

        List<Row> results = resultDF.collectAsList();
        assertEquals(1, results.size());
        Row row = results.get(0);
        assertEquals(documentHash.getHashValue(), row.getAs("hashValue"));
    }

    @Test
    public void storeInCacheShouldRunProperly() throws Exception {
        // given
        AvroDataFrameSupport avroDataFrameSupport = new AvroDataFrameSupport(spark);
        DocumentHashToProject documentHashToProject = DocumentHashToProject.newBuilder()
                .setHashValue("a1")
                .setProjectId("projId-1")
                .setConfidenceLevel(1.0f)
                .build();
        List<DocumentHashToProject> documentHashToProjectList = Collections.singletonList(documentHashToProject);
        Dataset<Row> documentHashToProjectDF = avroDataFrameSupport.createDataFrame(
                documentHashToProjectList,
                DocumentHashToProject.SCHEMA$);
        DocumentHash documentHash = DocumentHash.newBuilder().setHashValue("a1").build();
        List<DocumentHash> documentHashList = Collections.singletonList(documentHash);
        Dataset<Row> documentHashDF = avroDataFrameSupport.createDataFrame(
                documentHashList,
                DocumentHash.SCHEMA$);
        LockManager lockManager = mock(LockManager.class);
        CacheMetadataManagingProcess cacheManager = mock(CacheMetadataManagingProcess.class);
        when(cacheManager.generateNewCacheId(any(Configuration.class), eq("path/to/cache"))).thenReturn("01");
        AvroDataStoreWriter writer = mock(AvroDataStoreWriter.class);

        // when
        storeInCache(spark,
                documentHashToProjectDF,
                documentHashDF,
                "path/to/cache",
                lockManager,
                cacheManager,
                10,
                writer);

        // then
        ArgumentCaptor<Dataset<Row>> dataFrameCaptor = new ArgumentCaptor<>();

        verify(lockManager, atLeastOnce()).obtain("path/to/cache");

        verify(writer, atLeastOnce()).write(dataFrameCaptor.capture(),
                eq(String.format("path/to/cache/01/%s",
                        CachedTaraReferenceExtractionJob.CacheRecordType.documentHashToProject.name())),
                eq(DocumentHashToProject.SCHEMA$));

        Dataset<Row> documentHashToProjectStoredDF = dataFrameCaptor.getValue();
        assertEquals(SchemaConverters.toSqlType(DocumentHashToProject.SCHEMA$).dataType(), documentHashToProjectStoredDF.schema());
        List<Row> documentHashToProjectRows = documentHashToProjectStoredDF.collectAsList();
        assertEquals(1, documentHashToProjectRows.size());
        Row documentHashToProjectRow = documentHashToProjectRows.get(0);
        assertEquals(documentHashToProject.getHashValue(), documentHashToProjectRow.getAs("hashValue"));
        assertEquals(documentHashToProject.getProjectId(), documentHashToProjectRow.getAs("projectId"));
        assertEquals(documentHashToProject.getConfidenceLevel(), documentHashToProjectRow.<Float>getAs("confidenceLevel"), 1e-3);
        assertNull(documentHashToProjectRow.getAs("textsnippet"));

        verify(writer, atLeastOnce()).write(dataFrameCaptor.capture(),
                eq(String.format("path/to/cache/01/%s",
                        CachedTaraReferenceExtractionJob.CacheRecordType.documentHash.name())),
                eq(DocumentHash.SCHEMA$));

        Dataset<Row> documentHashStoredDF = dataFrameCaptor.getValue();
        assertEquals(SchemaConverters.toSqlType(DocumentHash.SCHEMA$).dataType(), documentHashStoredDF.schema());
        List<Row> documentHashRows = documentHashStoredDF.collectAsList();
        assertEquals(1, documentHashRows.size());
        Row documentHashRow = documentHashRows.get(0);
        assertEquals(documentHash.getHashValue(), documentHashRow.getAs("hashValue"));

        verify(cacheManager, atLeastOnce()).writeCacheId(any(Configuration.class), eq("path/to/cache"), eq("01"));
        verify(lockManager, atLeastOnce()).release("path/to/cache");
    }

    @Test
    public void storeInOutputShouldRunProperly() {
        // given
        DocumentToProject documentToProject = DocumentToProject.newBuilder()
                .setDocumentId("docId-1")
                .setProjectId("projId-1")
                .setConfidenceLevel(1.0f)
                .build();
        List<DocumentToProject> documentToProjectList = Collections.singletonList(documentToProject);
        Dataset<Row> documentToProjectDF = new AvroDataFrameSupport(spark).createDataFrame(
                documentToProjectList,
                DocumentToProject.SCHEMA$);
        AvroDataStoreWriter writer = mock(AvroDataStoreWriter.class);

        // when
        storeInOutput(documentToProjectDF, "path/to/output", writer);

        // then
        ArgumentCaptor<Dataset<Row>> dataFrameCaptor = new ArgumentCaptor<>();
        verify(writer, atLeastOnce()).write(dataFrameCaptor.capture(),
                eq("path/to/output"),
                eq(DocumentToProject.SCHEMA$));
        Dataset<Row> documentToProjectStoredDF = dataFrameCaptor.getValue();
        assertEquals(SchemaConverters.toSqlType(DocumentToProject.SCHEMA$).dataType(), documentToProjectStoredDF.schema());
        List<Row> documentToProjectRows = documentToProjectStoredDF.collectAsList();
        assertEquals(1, documentToProjectRows.size());
        Row documentToProjecRow = documentToProjectRows.get(0);
        assertEquals(documentToProject.getDocumentId(), documentToProjecRow.getAs("documentId"));
        assertEquals(documentToProject.getProjectId(), documentToProjecRow.getAs("projectId"));
        assertEquals(documentToProject.getConfidenceLevel(), documentToProjecRow.<Float>getAs("confidenceLevel"), 1e-3);
        assertNull(documentToProjecRow.getAs("textsnippet"));
    }
}