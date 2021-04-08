package eu.dnetlib.iis.common.spark.avro;

import eu.dnetlib.iis.common.avro.Person;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;
import eu.dnetlib.iis.common.utils.AvroTestUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AvroDataFrameWriterTest extends TestWithSharedSparkSession {

    @BeforeEach
    public void beforeEach() {
        super.beforeEach();
    }

    @Test
    @DisplayName("Avro dataframe writer writes dataframe of avro type using SQL schema")
    public void givenDataFrameOfAvroType_whenWrittenToOutputUsingSQLSchema_thenWriteSucceeds(@TempDir Path workingDir) throws IOException {
        Path outputDir = workingDir.resolve("output");
        Row personRow = RowFactory.create(1, "name", 2);
        Dataset<Row> df = spark().createDataFrame(
                Collections.singletonList(personRow),
                (StructType) SchemaConverters.toSqlType(Person.SCHEMA$).dataType()
        );

        new AvroDataFrameWriter(df).write(outputDir.toString());

        List<GenericRecord> genericRecordList = AvroTestUtils.readLocalAvroDataStore(outputDir.toString());
        assertEquals(1, genericRecordList.size());
        GenericRecord genericRecord = genericRecordList.get(0);
        assertEquals(personRow.getAs(0), genericRecord.get(0));
        assertEquals(personRow.getAs(1).toString(), genericRecord.get(1).toString());
        assertEquals(personRow.getAs(2), genericRecord.get(2));
    }

    @Test
    @DisplayName("Avro dataframe writer writes dataframe of avro type using avro schema")
    public void givenDataFrameOfAvroType_whenWrittenToOutputUsingAvroSchema_thenWriteSucceeds(@TempDir Path workingDir) throws IOException {
        Path outputDir = workingDir.resolve("output");
        Row personRow = RowFactory.create(1, "name", 2);
        Dataset<Row> df = spark().createDataFrame(
                Collections.singletonList(personRow),
                (StructType) SchemaConverters.toSqlType(Person.SCHEMA$).dataType()
        );

        new AvroDataFrameWriter(df).write(outputDir.toString(), Person.SCHEMA$);

        List<Person> personList = AvroTestUtils.readLocalAvroDataStore(outputDir.toString());
        assertEquals(1, personList.size());
        Person person = personList.get(0);
        assertEquals(personRow.getAs(0), person.getId());
        assertEquals(personRow.getAs(1), person.getName());
        assertEquals(personRow.getAs(2), person.getAge());
    }
}