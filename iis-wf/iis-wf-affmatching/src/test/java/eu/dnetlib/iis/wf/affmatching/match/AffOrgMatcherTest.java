package eu.dnetlib.iis.wf.affmatching.match;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgHashBucketJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import scala.Tuple2;

/**
* @author Łukasz Dumiszewski
*/
@RunWith(MockitoJUnitRunner.class)
public class AffOrgMatcherTest {

    
    @InjectMocks
    private AffOrgMatcher matcher = new AffOrgMatcher();
    
    @Mock
    private AffOrgJoiner affOrgJoiner = new AffOrgHashBucketJoiner();
    
    @Mock
    private AffOrgMatchComputer affOrgMatchComputer = new AffOrgMatchComputer();
    
    @Mock
    private BestAffMatchResultPicker bestAffMatchResultPicker = new BestAffMatchResultPicker();
    
    @Mock
    private JavaRDD<AffMatchAffiliation> affiliations;
    
    @Mock
    private JavaRDD<AffMatchOrganization> organizations; 
    
    @Mock
    private JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> joinedAffOrgs;
    
    @Mock
    private JavaRDD<AffMatchResult> matchedAffOrgs;
    
    @Mock
    private JavaRDD<AffMatchResult> bestMatchedAffOrgs;
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test(expected = NullPointerException.class)
    public void match_affiliations_null() {
        
        // execute
        
        matcher.match(null, organizations);
        
    }

    
    @Test(expected = NullPointerException.class)
    public void match_organizations_null() {
        
        // execute
        
        matcher.match(affiliations, null);
        
    }


    @Test(expected = NullPointerException.class)
    public void match_affOrgMatchComputer_null() {
        
        // given
        
        matcher.setAffOrgMatchComputer(null);
        
        
        // execute
        
        matcher.match(affiliations, organizations);
        
    }

    
    @Test(expected = NullPointerException.class)
    public void match_affOrgJoiner_null() {
        
        // given
        
        matcher.setAffOrgJoiner(null);
        
        
        // execute
        
        matcher.match(affiliations, organizations);
        
    }

    
    @Test
    public void match() {
        
        // given
        
        doReturn(joinedAffOrgs).when(affOrgJoiner).join(affiliations, organizations);
        doReturn(matchedAffOrgs).when(affOrgMatchComputer).computeMatches(joinedAffOrgs);
        doReturn(bestMatchedAffOrgs).when(bestAffMatchResultPicker).pickBestAffMatchResults(matchedAffOrgs);
        
        
        // execute
        
        JavaRDD<AffMatchResult> affMatchResults = matcher.match(affiliations, organizations);
        
        
        // assert
        
        assertTrue(affMatchResults == bestMatchedAffOrgs);
        
    }
    
    
    
    
    
    
    

}
