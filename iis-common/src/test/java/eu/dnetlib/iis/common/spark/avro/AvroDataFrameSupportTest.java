package eu.dnetlib.iis.common.spark.avro;

import eu.dnetlib.iis.common.SlowTest;
import eu.dnetlib.iis.common.avro.Person;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SlowTest
public class AvroDataFrameSupportTest {

    private static SparkSession spark;
    private static AvroDataFrameSupport avroDataFrameSupport;

    @TempDir
    public static Path workingDir;

    @BeforeAll
    public static void beforeAll() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.set("spark.driver.host", "localhost");
        conf.setAppName(AvroDataFrameSupportTest.class.getSimpleName());
        spark = SparkSession.builder().config(conf).getOrCreate();
        avroDataFrameSupport = new AvroDataFrameSupport(spark);
    }

    @AfterAll
    public static void afterAll() {
        spark.stop();
    }

    @Test
    public void createDataFrameShouldRunProperly() {
        // given
        Person person = Person.newBuilder().setId(1).setName("name").setAge(2).build();
        List<Person> data = Collections.singletonList(person);

        // when
        Dataset<Row> result = avroDataFrameSupport.createDataFrame(data, Person.SCHEMA$);

        // then
        assertSchemasEqualIgnoringNullability(Person.SCHEMA$, result.schema());
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(person.getId(), row.getAs("id"));
        assertEquals(person.getName(), row.getAs("name"));
        assertEquals(person.getAge(), row.getAs("age"));
    }

    @Test
    public void readShouldReadProperlyUsingSqlSchema() throws IOException {
        // given
        Path inputDir = workingDir.resolve("input");
        Person person = Person.newBuilder().setId(1).setName("name").setAge(2).build();
        List<Person> data = Collections.singletonList(person);
        AvroTestUtils.createLocalAvroDataStore(data, inputDir.toString(), Person.class);

        // when
        Dataset<Row> result = avroDataFrameSupport.read(inputDir.toString(),
                (StructType) SchemaConverters.toSqlType(Person.SCHEMA$).dataType());

        // then
        assertSchemasEqualIgnoringNullability(Person.SCHEMA$, result.schema());
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(person.getId(), row.getAs("id"));
        assertEquals(person.getName(), row.getAs("name"));
        assertEquals(person.getAge(), row.getAs("age"));
    }

    @Test
    public void readShouldReadProperlyUsingAvroSchema() throws IOException {
        // given
        Path inputDir = workingDir.resolve("input");
        Person person = Person.newBuilder().setId(1).setName("name").setAge(2).build();
        List<Person> data = Collections.singletonList(person);
        AvroTestUtils.createLocalAvroDataStore(data, inputDir.toString(), Person.class);

        // when
        Dataset<Row> result = avroDataFrameSupport.read(inputDir.toString(), Person.SCHEMA$);

        // then
        assertSchemasEqualIgnoringNullability(Person.SCHEMA$, result.schema());
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(person.getId(), row.getAs("id"));
        assertEquals(person.getName(), row.getAs("name"));
        assertEquals(person.getAge(), row.getAs("age"));
    }

    @Test
    public void writeShouldRunProperlyUsingSqlSchema() throws IOException {
        // given
        Path outputDir = workingDir.resolve("output1");
        Row personRow = RowFactory.create(1, "name", 2);
        Dataset<Row> df = spark.createDataFrame(
                Collections.singletonList(personRow),
                (StructType) SchemaConverters.toSqlType(Person.SCHEMA$).dataType()
        );

        // when
        avroDataFrameSupport.write(df, outputDir.toString());

        // then
        List<GenericRecord> genericRecordList = AvroTestUtils.readLocalAvroDataStore(outputDir.toString());
        assertEquals(1, genericRecordList.size());
        GenericRecord genericRecord = genericRecordList.get(0);
        assertEquals(personRow.getAs(0), genericRecord.get(0));
        assertEquals(personRow.getAs(1).toString(), genericRecord.get(1).toString());
        assertEquals(personRow.getAs(2), genericRecord.get(2));
    }

    @Test
    public void writeShouldRunProperlyUsingAvroSchema() throws IOException {
        // given
        Path outputDir = workingDir.resolve("output2");
        Row personRow = RowFactory.create(1, "name", 2);
        Dataset<Row> df = spark.createDataFrame(
                Collections.singletonList(personRow),
                (StructType) SchemaConverters.toSqlType(Person.SCHEMA$).dataType()
        );

        // when
        avroDataFrameSupport.write(df, outputDir.toString(), Person.SCHEMA$);

        // then
        List<Person> personList = AvroTestUtils.readLocalAvroDataStore(outputDir.toString());
        assertEquals(1, personList.size());
        Person person = personList.get(0);
        assertEquals(personRow.getAs(0), person.getId());
        assertEquals(personRow.getAs(1), person.getName());
        assertEquals(personRow.getAs(2), person.getAge());
    }

    @Test
    public void toDSShouldRunProperly() {
        // given
        Row personRow = RowFactory.create(1, "name", 2);
        List<Row> data = Collections.singletonList(personRow);
        Dataset<Row> df = spark.createDataFrame(
                data, (StructType) SchemaConverters.toSqlType(Person.SCHEMA$).dataType()
        );

        // when
        Dataset<Person> result = avroDataFrameSupport.toDS(df, Person.class);

        // then
        List<Person> personList = result.collectAsList();
        assertEquals(1, personList.size());
        Person person = personList.get(0);
        assertEquals(personRow.getAs(0), person.getId());
        assertEquals(personRow.getAs(1), person.getName());
        assertEquals(personRow.getAs(2), person.getAge());
    }

    public static void assertSchemasEqualIgnoringNullability(Schema avroSchema, StructType sqlSchema) {
        assertEquals(SchemaConverters.toSqlType(avroSchema).dataType().asNullable(), sqlSchema.asNullable());
    }
}