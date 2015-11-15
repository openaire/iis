package eu.dnetlib.iis.core.common;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;

import com.google.common.collect.Lists;
import com.google.gson.Gson;

/**
 * Utils class for operating on avro objects saved in json files
 * 
 * @author madryk
 *
 */
public class JsonTestUtils {


    //------------------------ CONSTRUCTORS --------------------------

    private JsonTestUtils() {
        throw new IllegalStateException("may not be instantiated");
    }


    //------------------------ LOGIC --------------------------

    /**
     * Reads avro objects from json file
     */
    public static <T extends GenericRecord> List<T> readJsonDataStore(String jsonFilePath, Class<T> recordsClass) throws IOException {
        Gson gson = AvroGsonFactory.create();
        List<T> jsonDatastore = Lists.newArrayList();

        try (FileReader reader = new FileReader(jsonFilePath)) {

            List<String> recordsStrings = IOUtils.readLines(reader);

            for (String recordString : recordsStrings) {
                T element = gson.fromJson(recordString, recordsClass);
                jsonDatastore.add(element);
            }
        }

        return jsonDatastore;
    }

}
