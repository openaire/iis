package eu.dnetlib.iis.common.spark.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import eu.dnetlib.iis.common.avro.Person;
import eu.dnetlib.iis.common.spark.TestWithSharedSparkSession;

public class AvroDataFrameSupportTest extends TestWithSharedSparkSession {

    @Test
    @DisplayName("Avro dataframe support converts dataframe of avro type to dataset of avro type")
    public void givenDataFrameOfAvroType_whenConvertedToDataset_thenProperDatasetIsReturned() {
        Row personRow = RowFactory.create(1, "name", 2);
        List<Row> data = Collections.singletonList(personRow);
        Dataset<Row> df = spark().createDataFrame(
                data, (StructType) SchemaConverters.toSqlType(Person.SCHEMA$).dataType()
        );

        Dataset<Person> result = AvroDataFrameSupport.toDS(df, Person.class);

        List<Person> personList = result.collectAsList();
        assertEquals(1, personList.size());
        Person person = personList.get(0);
        assertEquals(personRow.getAs(0), person.getId());
        assertEquals(personRow.getAs(1), person.getName());
        assertEquals(personRow.getAs(2), person.getAge());
    }

}