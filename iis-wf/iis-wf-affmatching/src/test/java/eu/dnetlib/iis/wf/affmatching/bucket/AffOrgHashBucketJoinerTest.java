package eu.dnetlib.iis.wf.affmatching.bucket;

import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.Tuple2;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

/**
* @author Łukasz Dumiszewski
*/
@ExtendWith(MockitoExtension.class)
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

    // --- hot-key detection chain: affiliation side ---
    @Mock
    private JavaRDD<String> affHashKeys;
    @Mock
    private JavaPairRDD<String, Long> affHashKeysCount;
    @Mock
    private JavaPairRDD<String, Long> affCounts;

    // --- hot-key detection chain: organization side ---
    @Mock
    private JavaRDD<String> orgHashKeys;
    @Mock
    private JavaPairRDD<String, Long> orgHashKeysCount;
    @Mock
    private JavaPairRDD<String, Long> orgCounts;

    // --- hot-key detection chain: cross-product and filter ---
    @Mock
    private JavaPairRDD<String, Tuple2<Long, Long>> affOrgCounts;
    @Mock
    private JavaPairRDD<String, Tuple2<Long, Long>> filteredAffOrgCounts;
    @Mock
    private JavaRDD<String> hotKeysRdd;

    // --- fast-path join result ---
    @Mock
    private JavaPairRDD<String, Tuple2<AffMatchAffiliation, AffMatchOrganization>> joinedHashAffOrganizations;
    
    @Mock
    private JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> joinedAffOrganizations;
    
    @Captor
    private ArgumentCaptor<PairFunction<AffMatchAffiliation, String, AffMatchAffiliation>> affHashFunction;

    @Captor
    private ArgumentCaptor<PairFunction<AffMatchOrganization, String, AffMatchOrganization>> orgHashFunction;

    
    
    //------------------------ TESTS --------------------------

    /**
     * Tests the no-hot-buckets (cold) path: when the cross-product of every bucket is below
     * {@link AffOrgHashBucketJoiner#MAX_BUCKET_CROSS_PRODUCT}, the plain join is used without salting.
     * <p>
     * The salted (hot-bucket) path requires a real SparkContext for broadcasting and is covered
     * by integration tests (e.g. AffMatchingRealLifeExampleJobTest).
     */
    @Test
    public void join_noHotBuckets_usesPlainJoin() throws Exception {
        
        // given

        doReturn(hashAffiliations).when(affiliations).mapToPair(Mockito.any());
        doReturn(hashOrganizations).when(organizations).mapToPair(Mockito.any());

        // hot-key detection: affiliation bucket counts
        doReturn(affHashKeys).when(hashAffiliations).keys();
        doReturn(affHashKeysCount).when(affHashKeys).mapToPair(Mockito.any());
        doReturn(affCounts).when(affHashKeysCount).reduceByKey(Mockito.any());

        // hot-key detection: organization bucket counts
        doReturn(orgHashKeys).when(hashOrganizations).keys();
        doReturn(orgHashKeysCount).when(orgHashKeys).mapToPair(Mockito.any());
        doReturn(orgCounts).when(orgHashKeysCount).reduceByKey(Mockito.any());

        // hot-key detection: cross-product filter returns empty — no hot buckets
        doReturn(affOrgCounts).when(affCounts).join(orgCounts);
        doReturn(filteredAffOrgCounts).when(affOrgCounts).filter(Mockito.any());
        doReturn(hotKeysRdd).when(filteredAffOrgCounts).keys();
        doReturn(Collections.emptyList()).when(hotKeysRdd).collect();

        // fast path
        doReturn(joinedHashAffOrganizations).when(hashAffiliations).join(hashOrganizations);
        doReturn(joinedAffOrganizations).when(joinedHashAffOrganizations).values();
        
        
        // execute
        
        JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> retJoinedAffOrgs = affOrgJoiner.join(affiliations, organizations);
        
        
        // assert

        assertSame(joinedAffOrganizations, retJoinedAffOrgs);
       
        verify(affiliations).mapToPair(affHashFunction.capture());
        assertAffHashFunction(affHashFunction.getValue());
        
        verify(organizations).mapToPair(orgHashFunction.capture());
        assertOrgHashFunction(orgHashFunction.getValue());

        verify(hashAffiliations).join(hashOrganizations);
        verify(joinedHashAffOrganizations).values();
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
        assertSame(aff, hashAff._2());
    }
    
    
    private void assertOrgHashFunction(PairFunction<AffMatchOrganization, String, AffMatchOrganization> function) throws Exception {
        
        // given
        
        AffMatchOrganization org = new AffMatchOrganization("ORG1");
        when(organizationBucketHasher.hash(org)).thenReturn("HASH");
        
        
        // execute
        
        Tuple2<String, AffMatchOrganization> hashOrg = function.call(org);
        
        
        // assert
        
        assertEquals("HASH", hashOrg._1());
        assertSame(org, hashOrg._2());
    }
    
    
}

    