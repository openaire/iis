package eu.dnetlib.iis.wf.affmatching.orgalternativenames;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.google.common.collect.Lists;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Factory of alternative names dictionary. As source of dictionary
 * it uses csv files located in the classpath.
 * 
 * @author madryk
 */
public class CsvOrganizationAltNamesDictionaryFactory {
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Creates dictionary of alternative organization names using csv files located 
     * under provided classpaths.
     */
    public List<List<String>> createAlternativeNamesDictionary(List<String> classpathCsvResources) throws IOException {
        
        List<List<String>> dictionary = Lists.newArrayList();
        
        for (String resource : classpathCsvResources) {
            
            List<List<String>> dictionaryFromCsv = createAltNamesDictionaryFromCsv(resource);
            dictionary.addAll(dictionaryFromCsv);
            
        }
        return dictionary;
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private List<List<String>> createAltNamesDictionaryFromCsv(String csvClasspath) throws IOException {
        
        List<List<String>> dictionary = Lists.newArrayList();
        
        try (CSVReader reader = new CSVReader(new InputStreamReader(this.getClass().getResourceAsStream(csvClasspath), "UTF-8"))) {
            
            String[] next = reader.readNext();
            while (next != null) {
                dictionary.add(Lists.newArrayList(next));
                next = reader.readNext();
            }
            
            
        }
        
        return dictionary;
    }
}
