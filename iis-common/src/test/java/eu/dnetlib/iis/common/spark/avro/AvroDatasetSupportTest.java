package eu.dnetlib.iis.common.spark.avro;

import eu.dnetlib.iis.common.avro.Person;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
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

public class AvroDatasetSupportTest {
    private static SparkSession spark;
    private static AvroDatasetSupport avroDatasetSupport;

    @TempDir
    static Path workingDir;

    @BeforeAll
    public static void beforeAll() {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.set("spark.driver.host", "localhost");
        conf.setAppName(AvroDatasetSupportTest.class.getSimpleName());
        spark = SparkSession.builder().config(conf).getOrCreate();
        avroDatasetSupport = new AvroDatasetSupport(spark);
    }

    @AfterAll
    public static void afterAll() {
        spark.stop();
    }

    @Test
    public void readShouldRunProperly() throws IOException {
        // given
        Path inputDir = workingDir.resolve("input");
        Person person = Person.newBuilder().setId(1).setName("name").setAge(2).build();
        List<Person> data = Collections.singletonList(person);
        AvroTestUtils.createLocalAvroDataStore(data, inputDir.toString(), Person.class);

        // when
        Dataset<Person> result = avroDatasetSupport.read(inputDir.toString(), Person.SCHEMA$, Person.class);

        // then
        List<Person> personList = result.collectAsList();
        assertEquals(1, personList.size());
        Person personRead = personList.get(0);
        assertEquals(person, personRead);
    }

    @Test
    public void writeShouldRunProperly() throws IOException {
        // given
        Path outputDir = workingDir.resolve("output");
        Person person = Person.newBuilder().setId(1).setName("name").setAge(2).build();
        Dataset<Person> ds = spark.createDataset(
                Collections.singletonList(person),
                Encoders.kryo(Person.class)
        );

        // when
        avroDatasetSupport.write(ds, outputDir.toString(), Person.SCHEMA$);

        // then
        List<Person> personList = AvroTestUtils.readLocalAvroDataStore(outputDir.toString());
        assertEquals(1, personList.size());
        Person personRead = personList.get(0);
        assertEquals(person, personRead);
    }

    @Test
    public void toDFShouldRunProperly() {
        // given
        Person person = Person.newBuilder().setId(1).setName("name").setAge(2).build();
        Dataset<Person> ds = spark.createDataset(
                Collections.singletonList(person),
                Encoders.kryo(Person.class)
        );

        // when
        Dataset<Row> result = avroDatasetSupport.toDF(ds, Person.SCHEMA$);

        // then
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