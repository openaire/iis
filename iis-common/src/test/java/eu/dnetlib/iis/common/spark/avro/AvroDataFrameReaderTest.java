package eu.dnetlib.iis.common.spark.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.avro.SchemaConverters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import eu.dnetlib.iis.common.avro.Person;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.common.utils.AvroTestUtils;

class AvroDataFrameReaderTest extends TestWithSharedSparkSession {

    private AvroDataFrameReader reader;

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
        reader = new AvroDataFrameReader(spark());
    }

    @Test
    @DisplayName("Avro dataframe reader reads avro datastore with avro schema as dataframe")
    public void givenAvroDatastore_whenReadUsingAvroReaderWithAvroSchema_thenProperDataFrameIsReturned(@TempDir Path inputDir) throws IOException {
        Person person = Person.newBuilder().setId(1).setName("name").setAge(2).build();
        List<Person> data = Collections.singletonList(person);
        AvroTestUtils.createLocalAvroDataStore(data, inputDir.toString(), Person.class);

        Dataset<Row> result = reader.read(inputDir.toString(), Person.SCHEMA$);

        assertEquals(SchemaConverters.toSqlType(Person.SCHEMA$).dataType(), result.schema());
        List<Row> rows = result.collectAsList();
        assertEquals(1, rows.size());
        Row row = rows.get(0);
        assertEquals(person.getId(), row.getAs("id"));
        assertEquals(person.getName(), row.getAs("name"));
        assertEquals(person.getAge(), row.getAs("age"));
    }
}