package eu.dnetlib.iis.common.spark.avro;

import eu.dnetlib.iis.common.avro.Person;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroDataFrameSupportTest extends TestWithSharedSparkSession {

    private AvroDataFrameSupport support;

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
        support = new AvroDataFrameSupport(spark());
    }

    @Test
    @DisplayName("Avro dataframe support creates dataframe from collection of avro type")
    public void givenACollectionOfAvroType_whenConvertedToDataFrame_thenProperDataFrameIsReturned() {
        Person person = Person.newBuilder().setId(1).setName("name").setAge(2).build();
        List<Person> data = Collections.singletonList(person);

        Dataset<Row> result = support.createDataFrame(data, Person.SCHEMA$);

        assertSchemasEqualIgnoringNullability(Person.SCHEMA$, result.schema());
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(person.getId(), row.getAs("id"));
        assertEquals(person.getName(), row.getAs("name"));
        assertEquals(person.getAge(), row.getAs("age"));
    }

    @Test
    @DisplayName("Avro dataframe support converts dataframe of avro type to dataset of avro type")
    public void givenDataFrameOfAvroType_whenConvertedToDataset_thenProperDatasetIsReturned() {
        Row personRow = RowFactory.create(1, "name", 2);
        List<Row> data = Collections.singletonList(personRow);
        Dataset<Row> df = spark().createDataFrame(
                data, (StructType) SchemaConverters.toSqlType(Person.SCHEMA$).dataType()
        );

        Dataset<Person> result = support.toDS(df, Person.class);

        List<Person> personList = result.collectAsList();
        assertEquals(1, personList.size());
        Person person = personList.get(0);
        assertEquals(personRow.getAs(0), person.getId());
        assertEquals(personRow.getAs(1), person.getName());
        assertEquals(personRow.getAs(2), person.getAge());
    }

    private static void assertSchemasEqualIgnoringNullability(Schema avroSchema, StructType sqlSchema) {
        assertEquals(SchemaConverters.toSqlType(avroSchema).dataType().asNullable(), sqlSchema.asNullable());
    }
}