package eu.dnetlib.iis.common.spark.avro;

import eu.dnetlib.iis.common.avro.Person;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import org.apache.spark.sql.Dataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroDatasetReaderTest extends TestWithSharedSparkSession {

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
    }

    @Test
    @DisplayName("Avro dataset reader reads avro datastore as dataset")
    public void givenAvroDatastore_whenReadUsingAvroReader_thenProperDataFrameIsReturned(@TempDir Path workingDir) throws IOException {
        Path inputDir = workingDir.resolve("input");
        Person person = Person.newBuilder().setId(1).setName("name").setAge(2).build();
        List<Person> data = Collections.singletonList(person);
        AvroTestUtils.createLocalAvroDataStore(data, inputDir.toString(), Person.class);

        Dataset<Person> result = new AvroDatasetReader(spark()).read(inputDir.toString(), Person.SCHEMA$, Person.class);

        List<Person> personList = result.collectAsList();
        assertEquals(1, personList.size());
        Person personRead = personList.get(0);
        assertEquals(person, personRead);
    }
}