package eu.dnetlib.iis.common.spark.avro;

import eu.dnetlib.iis.common.avro.Person;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroDatasetWriterTest extends TestWithSharedSparkSession {

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
    }

    @Test
    @DisplayName("Avro dataset writer writes dataset of avro type")
    public void givenDatasetOfAvroType_whenWrittenToOutput_thenWriteSucceeds(@TempDir Path workingDir) throws IOException {
        Path outputDir = workingDir.resolve("output");
        Person person = Person.newBuilder().setId(1).setName("name").setAge(2).build();
        Dataset<Person> ds = spark().createDataset(
                Collections.singletonList(person),
                Encoders.kryo(Person.class)
        );

        new AvroDatasetWriter<>(ds).write(outputDir.toString(), Person.SCHEMA$);

        List<Person> personList = AvroTestUtils.readLocalAvroDataStore(outputDir.toString());
        assertEquals(1, personList.size());
        Person personRead = personList.get(0);
        assertEquals(person, personRead);
    }
}