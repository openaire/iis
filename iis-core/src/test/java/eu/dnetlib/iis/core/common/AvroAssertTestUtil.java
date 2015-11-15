package eu.dnetlib.iis.core.common;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;


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
     * Asserts equality of avro datastore to json datastore
     */
    public static <T extends GenericRecord> void assertEqualsWithJson(String avroDatastorePath, String jsonDatastorePath, Class<T> recordsClass) throws IOException {

        List<T> avroDatastore = AvroTestUtils.readLocalAvroDataStore(avroDatastorePath);

        List<T> jsonDatastore = JsonTestUtils.readJsonDataStore(jsonDatastorePath, recordsClass);

        assertEquals(
                jsonDatastore.stream().map(T::toString).collect(Collectors.toList()),
                avroDatastore.stream().map(T::toString).collect(Collectors.toList()));

    }

}
