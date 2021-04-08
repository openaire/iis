package eu.dnetlib.iis.common.spark.avro;

import eu.dnetlib.iis.common.avro.Person;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroDatasetSupportTest extends TestWithSharedSparkSession {

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
    }

    @Test
    @DisplayName("Avro dataset support converts dataset of avro type to dataframe of avro type")
    public void givenDatasetOfAvroType_whenConvertedToDataFrame_thenProperDataFrameIsReturned() {
        Person person = Person.newBuilder().setId(1).setName("name").setAge(2).build();
        Dataset<Person> ds = spark().createDataset(
                Collections.singletonList(person),
                Encoders.kryo(Person.class)
        );

        Dataset<Row> result = new AvroDatasetSupport(spark()).toDF(ds, Person.SCHEMA$);

        assertSchemasEqualIgnoringNullability(Person.SCHEMA$, result.schema());
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(person.getId(), row.getAs("id"));
        assertEquals(person.getName(), row.getAs("name"));
        assertEquals(person.getAge(), row.getAs("age"));
    }

    public static void assertSchemasEqualIgnoringNullability(Schema avroSchema, StructType sqlSchema) {
        assertEquals(SchemaConverters.toSqlType(avroSchema).dataType().asNullable(), sqlSchema.asNullable());
    }
}