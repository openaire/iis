package eu.dnetlib.iis.wf.affmatching.match.voter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.orgsection.OrganizationSectionsSplitter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

/**
 * @author madryk, Łukasz Dumiszewski
 */
@ExtendWith(MockitoExtension.class)
public class CommonAffSectionWordsVoterTest {

    
    private double MIN_COMMON_WORDS_RATIO = 0.8;
    
    private int WORDS_TO_REMOVE_MAX_LENGTH = 2;
    
    private List<Character> CHARS_TO_FILTER = ImmutableList.of(',');
    
    
    @InjectMocks
    private CommonAffSectionWordsVoter voter = new CommonAffSectionWordsVoter(CHARS_TO_FILTER, WORDS_TO_REMOVE_MAX_LENGTH, MIN_COMMON_WORDS_RATIO);
    
    @Mock
    private OrganizationSectionsSplitter organizationSectionsSplitter;
    
    @Mock
    private StringFilter stringFilter;
    
    @Mock
    private CommonSimilarWordCalculator commonSimilarWordCalculator;
    
    @Mock
    private Function<AffMatchOrganization, List<String>> getOrgNamesFunction;
    
    
    private AffMatchAffiliation affiliation = new AffMatchAffiliation("DOC_ID", 1);
    
    private String affOrgName = "AFF_ORG_NAME";
    
    private AffMatchOrganization organization = new AffMatchOrganization("ORG_ID");
    
    
    @BeforeEach
    public void setup() {
        
        affiliation.setOrganizationName(affOrgName);
           
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void constructor_NULL_CHARS_TO_FILTER() {
        
        // execute
        assertThrows(NullPointerException.class, () ->
                new CommonAffSectionWordsVoter(null, 2, 0.8));
    
    }
    
    
    @Test
    public void constructor_NEGATIVE_MIN_WORD_LENGTH() {

        // execute
        assertThrows(IllegalArgumentException.class, () ->
                new CommonAffSectionWordsVoter(of(), -1, 0.8));
        
    }
    
    
    @Test
    public void constructor_TOO_HIGH_MIN_COMMON_WORDS_RATIO() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                new CommonAffSectionWordsVoter(of(), 2, 1.1));
        
    }
    
    
    @Test
    public void constructor_TOO_LOW_MIN_COMMON_WORDS_RATIO() {
        
        // execute
        assertThrows(IllegalArgumentException.class, () ->
                new CommonAffSectionWordsVoter(of(), 2, 0));
        
    }
    
    
    @Test
    public void voteMatch_ONE_ORG_NAME__ENOUGH_COMMON_WORDS() {
        
        // given
        
        resetOrgNames("University of Toronto");
        
        when(organizationSectionsSplitter.splitToSections(affOrgName)).thenReturn(newArrayList("Chemistry Department", "Universiti of Toronto"));
        
        whenFilterCharsThen("University of Toronto", "University Toronto");
        
        whenFilterCharsThen("Chemistry Department", "Chemistry Department");
        whenFilterCharsThen("Universiti of Toronto", "Universiti Toronto");
        
        
        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Chemistry", "Department"), newArrayList("University", "Toronto")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO - 0.01);

        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Universiti", "Toronto"), newArrayList("University", "Toronto")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO + 0.01);

        
        // execute & assert

        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    
    
    
    @Test
    public void voteMatch_MANY_ORG_NAMES__ENOUGH_COMMON_WORDS() {
        
        // given
        
        resetOrgNames("Technical University in Poznan", "University of Toronto", "University of Warsaw");
        
        when(organizationSectionsSplitter.splitToSections(affOrgName)).thenReturn(newArrayList("Chemistry Department", "Universiti of Toronto"));
        
        
        whenFilterCharsThen("Chemistry Department", "Chemistry Department");
        whenFilterCharsThen("Universiti of Toronto", "Universiti Toronto");
        
        whenFilterCharsThen("Technical University in Poznan", "Technical University Poznan");
        whenFilterCharsThen("University of Toronto", "University Toronto");

        
        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Chemistry", "Department"), newArrayList("Technical", "University", "Poznan")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO - 0.01);

        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Universiti", "Toronto"), newArrayList("Technical", "University", "Poznan")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO - 0.01);
        

        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Chemistry", "Department"), newArrayList("University", "Toronto")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO - 0.01);

        when(commonSimilarWordCalculator.calcSimilarWordRatio(newArrayList("Universiti", "Toronto"), newArrayList("University", "Toronto")))
                                        .thenReturn(MIN_COMMON_WORDS_RATIO + 0.01);


        // execute & assert
        
        assertTrue(voter.voteMatch(affiliation, organization));
    }
    
    
    @Test
    public void voteMatch_ONE_ORG_NAME__NOT_ENOUGH_COMMON_WORDS() {
        
        // given
        
        resetOrgNames("University of Warsaw");
        
        when(organizationSectionsSplitter.splitToSections(affOrgName)).thenReturn(newArrayList("Chemistry Department", "University of Toronto"));
        
        whenFilterCharsThen("University of Warsaw", "University Warsaw");
        
        whenFilterCharsThen("Chemistry Department", "Chemistry Department");
        whenFilterCharsThen("University of Toronto", "University Toronto");
        
        
        // execute & assert

        assertFalse(voter.voteMatch(affiliation, organization));
    }
    
        
    //------------------------ PRIVATE --------------------------
    
    private void resetOrgNames(String... orgNames) {
        Mockito.when(getOrgNamesFunction.apply(organization)).thenReturn(Lists.newArrayList(orgNames));
    }
    
    private void whenFilterCharsThen(String inValue, String outValue) {
        when(stringFilter.filterCharsAndShortWords(inValue, CHARS_TO_FILTER, WORDS_TO_REMOVE_MAX_LENGTH)).thenReturn(outValue);
    }
}
