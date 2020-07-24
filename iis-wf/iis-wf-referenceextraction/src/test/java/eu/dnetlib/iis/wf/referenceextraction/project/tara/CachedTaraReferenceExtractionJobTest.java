package eu.dnetlib.iis.wf.referenceextraction.project.tara;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CachedTaraReferenceExtractionJobTest {

    @Test
    public void documentMetadataByIdShouldHaveProperSchema() {
        // then
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_BY_ID_SCHEMA.fields()[0],
                "id", DataTypes.StringType, false);
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_BY_ID_SCHEMA.fields()[1],
                "title", DataTypes.StringType, true);
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_BY_ID_SCHEMA.fields()[2],
                "abstract", DataTypes.StringType, true);
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_BY_ID_SCHEMA.fields()[3],
                "text", DataTypes.StringType, false);
    }

    @Test
    public void documentMetadataByHashShouldHaveProperSchema() {
        // then
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_BY_HASH_SCHEMA.fields()[0],
                "hashValue", DataTypes.StringType, false);
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_BY_HASH_SCHEMA.fields()[1],
                "title", DataTypes.StringType, true);
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_BY_HASH_SCHEMA.fields()[2],
                "abstract", DataTypes.StringType, true);
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_BY_HASH_SCHEMA.fields()[3],
                "text", DataTypes.StringType, false);
    }

    @Test
    public void documentMetadataWithHashShouldHaveProperSchema() {
        // then
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA.fields()[0],
                "id", DataTypes.StringType, false);
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA.fields()[1],
                "title", DataTypes.StringType, true);
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA.fields()[2],
                "abstract", DataTypes.StringType, true);
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA.fields()[3],
                "text", DataTypes.StringType, false);
        assertForField(CachedTaraReferenceExtractionJob.DOCUMENT_METADATA_SCHEMA.fields()[4],
                "hashValue", DataTypes.StringType, false);
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