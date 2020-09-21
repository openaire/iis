package eu.dnetlib.iis.wf.affmatching.orgalternativenames;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import au.com.bytecode.opencsv.CSVReader;
import eu.dnetlib.iis.common.ClassPathResourceProvider;

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
    public List<Set<String>> createAlternativeNamesDictionary(List<String> classpathCsvResources) throws IOException {
        
        List<Set<String>> dictionary = Lists.newArrayList();
        
        for (String resource : classpathCsvResources) {
            
            List<Set<String>> dictionaryFromCsv = createAltNamesDictionaryFromCsv(resource);
            dictionary.addAll(dictionaryFromCsv);
            
        }
        return dictionary;
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private List<Set<String>> createAltNamesDictionaryFromCsv(String csvClasspath) throws IOException {
        
        List<Set<String>> dictionary = Lists.newArrayList();

        try (CSVReader reader = new CSVReader(ClassPathResourceProvider.getResourceReader(csvClasspath))) {
            
            String[] next = reader.readNext();
            while (next != null) {
                dictionary.add(Sets.newHashSet(next));
                next = reader.readNext();
            }
            
            
        }
        
        return dictionary;
    }
}
