package eu.dnetlib.iis.wf.affmatching.orgalternativenames;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;

/**
 * @author madryk
 */
public class AffMatchOrganizationAltNameFillerTest {

    private AffMatchOrganizationAltNameFiller altNameFiller = new AffMatchOrganizationAltNameFiller();
    
    private List<List<String>> altNamesDictionary = Lists.newArrayList();
    
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG_ID");
    
    
    @Before
    public void setup() {
        
        altNamesDictionary.add(ImmutableList.of("Uniwersytet im. Adama Mickiewicza w Poznaniu", "Adam Mickiewicz University in Poznań", "Adam Mickiewicz University"));
        altNamesDictionary.add(ImmutableList.of("Univerzita Karlova v Praze", "Charles University in Prague"));
        
        
        altNameFiller.setAlternativeNamesDictionary(altNamesDictionary);
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void fillAlternativeNames_BASED_ON_NAME() {
        
        // given
        organization.setName("Uniwersytet im. Adama Mickiewicza w Poznaniu");
        
        // execute
        AffMatchOrganization retOrganization = altNameFiller.fillAlternativeNames(organization);
        
        // assert
        assertThat(retOrganization.getAlternativeNames(), containsInAnyOrder(
                "Adam Mickiewicz University in Poznań", "Adam Mickiewicz University"));
    }
    
    @Test
    public void fillAlternativeNames_BASED_ON_ORIGINAL_ALTERNATIVE_NAME() {
        
        // given
        organization.setName("UAM");
        organization.addAlternativeName("Adam Mickiewicz University");
        
        // execute
        AffMatchOrganization retOrganization = altNameFiller.fillAlternativeNames(organization);
        
        // assert
        assertThat(retOrganization.getAlternativeNames(), containsInAnyOrder(
                "Uniwersytet im. Adama Mickiewicza w Poznaniu", "Adam Mickiewicz University in Poznań", "Adam Mickiewicz University"));
    }
    
    @Test
    public void fillAlternativeNames_KEEP_ORIGINAL_ALTERNATIVE_NAME() {

        // given
        organization.setName("Univerzita Karlova v Praze");
        organization.addAlternativeName("Charles University");
        
        // execute
        AffMatchOrganization retOrganization = altNameFiller.fillAlternativeNames(organization);
        
        // assert
        assertThat(retOrganization.getAlternativeNames(), containsInAnyOrder(
                "Charles University", "Charles University in Prague"));
    }
    
    @Test
    public void fillAlternativeNames_ALTERNATIVE_NAMES_NOT_FOUND() {
        
        // given
        organization.setName("Uniwersytet Jagielloński");
        organization.addAlternativeName("Jagiellonian University");
        
        // execute
        AffMatchOrganization retOrganization = altNameFiller.fillAlternativeNames(organization);
        
        // assert
        assertThat(retOrganization.getAlternativeNames(), containsInAnyOrder("Jagiellonian University"));
    }
    
}
