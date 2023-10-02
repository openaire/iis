package eu.dnetlib.iis.common.spark.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.avro.Schema;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;

import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;

public class AvroDatasetSupportTest extends TestWithSharedSparkSession {

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
    }

    public static void assertSchemasEqualIgnoringNullability(Schema avroSchema, StructType sqlSchema) {
        assertEquals(SchemaConverters.toSqlType(avroSchema).dataType().asNullable(), sqlSchema.asNullable());
    }
}