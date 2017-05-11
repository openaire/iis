package eu.dnetlib.iis.common.utils;

import static com.google.common.collect.Lists.newArrayList;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;


/**
 * Utils class for operating on objects saved in json files
 * 
 * @author madryk
 */
public final class JsonTestUtils {


    //------------------------ CONSTRUCTORS --------------------------

    private JsonTestUtils() {
        throw new IllegalStateException("may not be instantiated");
    }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Reads records from json file (single record must be saved in single line of file).
     */
    public static <T> List<T> readJson(String jsonFilePath, Class<T> recordClass) throws IOException {
        
        List<T> records = newArrayList();
        Gson gson = new Gson();
        
        
        try(BufferedReader reader = new BufferedReader(new FileReader(jsonFilePath))) {
            
            String line = reader.readLine();
            
            while(line != null) {
                
                if (StringUtils.isNotBlank(line)) {
                    T record = gson.fromJson(line, recordClass);
                    records.add(record);
                }
                
                line = reader.readLine();
            }
            
        }
        
        return records;
    }
    
    /**
     * Reads records from multiple json files (single record must be saved in single line of file).
     */
    public static <T> List<T> readMultipleJsons(List<String> jsonFilePaths, Class<T> recordClass) throws IOException {
        
        List<T> records = newArrayList();
        
        for (String jsonOutputPath : jsonFilePaths) {
            records.addAll(readJson(jsonOutputPath, recordClass));
        }
        
        return records;
    }
}
