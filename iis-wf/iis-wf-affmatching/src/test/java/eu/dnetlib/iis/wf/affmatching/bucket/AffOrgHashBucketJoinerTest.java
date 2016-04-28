package eu.dnetlib.iis.wf.affmatching.bucket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import scala.Tuple2;

/**
* @author Łukasz Dumiszewski
*/
@RunWith(MockitoJUnitRunner.class)
public class AffOrgHashBucketJoinerTest {

    
    @InjectMocks
    private AffOrgHashBucketJoiner affOrgJoiner = new AffOrgHashBucketJoiner();
    
    @Mock
    private BucketHasher<AffMatchAffiliation> affiliationBucketHasher;
    
    @Mock
    private BucketHasher<AffMatchOrganization> organizationBucketHasher;

    @Mock
    private JavaRDD<AffMatchAffiliation> affiliations;
    
    @Mock
    private JavaRDD<AffMatchOrganization> organizations;
    
    @Mock
    private JavaPairRDD<String, AffMatchAffiliation> hashAffiliations;
    
    @Mock
    private JavaPairRDD<String, AffMatchOrganization> hashOrganizations;
    
    @Mock
    private JavaPairRDD<String, Tuple2<AffMatchAffiliation, AffMatchOrganization>> joinedHashAffOrganizations;
    
    @Mock
    private JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> joinedAffOrganizations;
    
    @Captor
    private ArgumentCaptor<PairFunction<AffMatchAffiliation, String, AffMatchAffiliation>> affHashFunction;

    @Captor
    private ArgumentCaptor<PairFunction<AffMatchOrganization, String, AffMatchOrganization>> orgHashFunction;

    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void join() throws Exception {
        
        
        // given
        
        doReturn(hashAffiliations).when(affiliations).mapToPair(Mockito.any());
        doReturn(hashOrganizations).when(organizations).mapToPair(Mockito.any());
        doReturn(joinedHashAffOrganizations).when(hashAffiliations).join(hashOrganizations);
        doReturn(joinedAffOrganizations).when(joinedHashAffOrganizations).values();
        
        
        // execute
        
        JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> retJoinedAffOrgs = affOrgJoiner.join(affiliations, organizations);  
        
        
        // assert
        
        assertTrue(joinedAffOrganizations == retJoinedAffOrgs);
       
        verify(affiliations).mapToPair(affHashFunction.capture());
        assertAffHashFunction(affHashFunction.getValue());
        
        verify(organizations).mapToPair(orgHashFunction.capture());
        assertOrgHashFunction(orgHashFunction.getValue());
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private void assertAffHashFunction(PairFunction<AffMatchAffiliation, String, AffMatchAffiliation> function) throws Exception {
        
        // given
        
        AffMatchAffiliation aff = new AffMatchAffiliation("DOC1", 1);
        when(affiliationBucketHasher.hash(aff)).thenReturn("HASH");
        
        
        // execute
        
        Tuple2<String, AffMatchAffiliation> hashAff = function.call(aff);
        
        
        // assert
        
        assertEquals("HASH", hashAff._1());
        assertTrue(aff == hashAff._2());
    }
    
    
    private void assertOrgHashFunction(PairFunction<AffMatchOrganization, String, AffMatchOrganization> function) throws Exception {
        
        // given
        
        AffMatchOrganization org = new AffMatchOrganization("ORG1");
        when(organizationBucketHasher.hash(org)).thenReturn("HASH");
        
        
        // execute
        
        Tuple2<String, AffMatchOrganization> hashOrg = function.call(org);
        
        
        // assert
        
        assertEquals("HASH", hashOrg._1());
        assertTrue(org == hashOrg._2());
    }
    
    
}

    