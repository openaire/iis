package eu.dnetlib.iis.common.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;

import eu.dnetlib.iis.common.utils.AvroTestUtils;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;


/**
 * Util class for asserting equality of avro and json datastores
 * 
 * @author madryk
 *
 */
public class AvroAssertTestUtil {


    //------------------------ CONSTRUCTORS --------------------------

    private AvroAssertTestUtil() {
        throw new IllegalStateException("may not be instantiated");
    }


    //------------------------ LOGIC --------------------------

    /**
     * Asserts equality of an avro datastore and a json datastore
     */
    public static <T extends GenericRecord> void assertEqualsWithJson(String avroDatastorePath, String jsonDatastorePath, Class<T> recordsClass) throws IOException {

        List<T> avroDatastore = AvroTestUtils.readLocalAvroDataStore(avroDatastorePath);

        List<T> jsonDatastore = JsonAvroTestUtils.readJsonDataStore(jsonDatastorePath, recordsClass);

        assertEquals(
                jsonDatastore.stream().map(T::toString).collect(Collectors.toList()),
                avroDatastore.stream().map(T::toString).collect(Collectors.toList()));

    }
    
    /**
     * Asserts equality of an avro datastore and a json datastore, ignores order of records
     */
    public static <T extends GenericRecord> void assertEqualsWithJsonIgnoreOrder(String avroDatastorePath, String jsonDatastorePath, Class<T> recordsClass) throws IOException {

        List<T> avroDatastore = AvroTestUtils.readLocalAvroDataStore(avroDatastorePath);

        List<T> jsonDatastore = JsonAvroTestUtils.readJsonDataStore(jsonDatastorePath, recordsClass);

        List<String> jsonStringDatastore = jsonDatastore.stream().map(T::toString).collect(Collectors.toList());
        List<String> avroStringDatastore = avroDatastore.stream().map(T::toString).collect(Collectors.toList());
        
        
        assertThat(avroStringDatastore, containsInAnyOrder(jsonStringDatastore.toArray()));

    }

}
