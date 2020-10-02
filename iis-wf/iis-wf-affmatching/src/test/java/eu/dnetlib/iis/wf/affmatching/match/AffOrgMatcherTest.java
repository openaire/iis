package eu.dnetlib.iis.wf.affmatching.match;

import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgHashBucketJoiner;
import eu.dnetlib.iis.wf.affmatching.bucket.AffOrgJoiner;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchResult;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;

/**
* @author Łukasz Dumiszewski
*/
@ExtendWith(MockitoExtension.class)
public class AffOrgMatcherTest {

    
    @InjectMocks
    private AffOrgMatcher matcher = new AffOrgMatcher();
    
    @Mock
    private AffOrgJoiner affOrgJoiner = new AffOrgHashBucketJoiner();
    
    @Mock
    private AffOrgMatchComputer affOrgMatchComputer = new AffOrgMatchComputer();
    
    @Mock
    private JavaRDD<AffMatchAffiliation> affiliations;
    
    @Mock
    private JavaRDD<AffMatchOrganization> organizations;
    
    @Mock
    private JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> joinedAffOrgs;
    
    @Mock
    private JavaRDD<AffMatchResult> matchedAffOrgs;
    
    
    
    //------------------------ TESTS --------------------------
    
    
    @Test
    public void match_affiliations_null() {
        
        // execute
        assertThrows(NullPointerException.class, () -> matcher.match(null, organizations));
        
    }

    
    @Test
    public void match_organizations_null() {
        
        // execute
        assertThrows(NullPointerException.class, () -> matcher.match(affiliations, null));

    }


    @Test
    public void match_affOrgMatchComputer_null() {
        
        // given
        
        matcher.setAffOrgMatchComputer(null);
        
        
        // execute
        
        assertThrows(NullPointerException.class, () -> matcher.match(affiliations, organizations));

    }

    
    @Test
    public void match_affOrgJoiner_null() {
        
        // given
        
        matcher.setAffOrgJoiner(null);
        
        
        // execute
        
        assertThrows(NullPointerException.class, () -> matcher.match(affiliations, organizations));

    }

    
    @Test
    public void match() {
        
        // given
        
        doReturn(joinedAffOrgs).when(affOrgJoiner).join(affiliations, organizations);
        doReturn(matchedAffOrgs).when(affOrgMatchComputer).computeMatches(joinedAffOrgs);
        
        
        // execute
        
        JavaRDD<AffMatchResult> affMatchResults = matcher.match(affiliations, organizations);
        
        
        // assert

        assertSame(affMatchResults, matchedAffOrgs);
        
    }
    
    
    
    
    
    
    

}
