package eu.dnetlib.iis.common.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.io.JsonStreamReader;

/**
 * Utils class for operating on avro objects saved in json files
 * 
 * @author madryk
 *
 */
public class JsonAvroTestUtils {


    //------------------------ CONSTRUCTORS --------------------------

    private JsonAvroTestUtils() {
        throw new IllegalStateException("may not be instantiated");
    }


    //------------------------ LOGIC --------------------------

    /**
     * Reads records from json file (jsonFilePath) as objects of recordClass class
     */
    public static <T extends GenericRecord> List<T> readJsonDataStore(String jsonFilePath, Class<T> avroRecordClass) throws IOException {
        List<T> jsonDatastore = Lists.newArrayList();

        Schema schema = AvroUtils.toSchema(avroRecordClass.getName());
        
        try (JsonStreamReader<T> reader = new JsonStreamReader<T>(schema, new FileInputStream(jsonFilePath), avroRecordClass)) {

            while(reader.hasNext()) {
                T record = reader.next();
            
                  jsonDatastore.add(record);
            }
        }

        return jsonDatastore;
    }

}
