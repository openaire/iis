package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import eu.dnetlib.iis.common.spark.avro.AvroDataFrameSupport;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static eu.dnetlib.iis.wf.referenceextraction.project.tara.TaraReferenceExtractionIOUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

public class TaraReferenceExtractionIOUtilsTest {

    private static SparkSession spark;

    @BeforeAll
    public static void beforeAll() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.set("spark.driver.host", "localhost");
        conf.setAppName(TaraReferenceExtractionIOUtilsTest.class.getSimpleName());
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    @AfterAll
    public static void afterAll() {
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
        ArgumentCaptor<Dataset<Row>> dataFrameCaptor = ArgumentCaptor.forClass(Dataset.class);
        verify(writer, atLeastOnce()).write(dataFrameCaptor.capture(),
                eq("path/to/output"),
                eq(DocumentToProject.SCHEMA$));
        Dataset<Row> documentToProjectStoredDF = dataFrameCaptor.getValue();
        assertEquals(SchemaConverters.toSqlType(DocumentToProject.SCHEMA$).dataType(), documentToProjectStoredDF.schema());
        List<Row> documentToProjectRows = documentToProjectStoredDF.collectAsList();
        assertEquals(1, documentToProjectRows.size());
        Row documentToProjectRow = documentToProjectRows.get(0);
        assertEquals(documentToProject.getDocumentId(), documentToProjectRow.getAs("documentId"));
        assertEquals(documentToProject.getProjectId(), documentToProjectRow.getAs("projectId"));
        assertEquals(documentToProject.getConfidenceLevel(), documentToProjectRow.<Float>getAs("confidenceLevel"), 1e-3);
        assertNull(documentToProjectRow.getAs("textsnippet"));
    }
}