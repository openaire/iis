package eu.dnetlib.iis.wf.referenceextraction;

import static eu.dnetlib.iis.wf.referenceextraction.ReferenceExtractionIOUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.referenceextraction.project.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.referenceextraction.ReferenceExtractionIOUtils.AvroDataStoreWriter;
import eu.dnetlib.iis.wf.referenceextraction.ReferenceExtractionIOUtils.OutputCleaner;

public class ReferenceExtractionIOUtilsTest extends TestWithSharedSparkSession {

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
        Dataset<Row> documentToProjectDF = createDataFrame(documentToProjectList);
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
    
    private Dataset<Row> createDataFrame(List<DocumentToProject> inputList) {
    	List<Row> dataFrameList = new ArrayList<>(inputList.size());
    	for (DocumentToProject input : inputList) {
			dataFrameList.add(RowFactory.create(input.getDocumentId(), input.getProjectId(), input.getConfidenceLevel(),
					input.getTextsnippet()));
    	}

    	Dataset<Row> result = spark().createDataFrame(dataFrameList, 
    			(StructType) SchemaConverters.toSqlType(DocumentToProject.SCHEMA$).dataType());

    	return result;
    }
    
}