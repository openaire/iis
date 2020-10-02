package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TaraReferenceExtractionJobTest {

    @Test
    public void documentMetadataShouldHaveProperSchema() {
        // then
        assertForField(TaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA.fields()[0],
                "id", DataTypes.StringType, false);
        assertForField(TaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA.fields()[1],
                "title", DataTypes.StringType, true);
        assertForField(TaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA.fields()[2],
                "abstract", DataTypes.StringType, true);
        assertForField(TaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA.fields()[3],
                "text", DataTypes.StringType, false);
    }

    private static void assertForField(StructField field,
                                       String name,
                                       DataType dataType,
                                       Boolean nullable) {
        assertEquals(name, field.name());
        assertEquals(dataType, field.dataType());
        assertEquals(nullable, field.nullable());
    }
}