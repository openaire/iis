package eu.dnetlib.iis.wf.affmatching.orgalternativenames;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author madryk
 */
public class CsvOrganizationAltNamesDictionaryFactoryTest {

    private CsvOrganizationAltNamesDictionaryFactory factory = new CsvOrganizationAltNamesDictionaryFactory();
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void createAlternativeNamesDictionary() throws IOException {
        
        // given
        List<String> csvFiles = ImmutableList.of(
                "/eu/dnetlib/iis/wf/affmatching/orgalternativenames/alternative_org_names_1.csv", 
                "/eu/dnetlib/iis/wf/affmatching/orgalternativenames/alternative_org_names_2.csv");
        
        // execute
        List<Set<String>> dictionary = factory.createAlternativeNamesDictionary(csvFiles);
        
        // assert
        assertEquals(3, dictionary.size());
        
        assertThat(dictionary.get(0), containsInAnyOrder("Uniwersytet im. Adama Mickiewicza w Poznaniu", "Adam Mickiewicz University in Poznań", "Adam Mickiewicz University"));
        assertThat(dictionary.get(1), containsInAnyOrder("Uniwersytet Jagielloński", "Jagiellonian University"));
        assertThat(dictionary.get(2), containsInAnyOrder("Sveučilište u Zagrebu", "University of Zagreb"));
        
    }
}
