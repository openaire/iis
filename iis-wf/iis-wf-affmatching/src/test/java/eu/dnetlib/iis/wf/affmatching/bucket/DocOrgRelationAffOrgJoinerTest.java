package eu.dnetlib.iis.wf.affmatching.bucket;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.*;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentOrganization;
import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read.DocumentOrganizationFetcher;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchAffiliation;
import eu.dnetlib.iis.wf.affmatching.model.AffMatchOrganization;
import scala.Tuple2;

/**
 * @author madryk
 */
@RunWith(MockitoJUnitRunner.class)
public class DocOrgRelationAffOrgJoinerTest {

    @InjectMocks
    private DocOrgRelationAffOrgJoiner docOrgRelationAffOrgJoiner = new DocOrgRelationAffOrgJoiner();
    
    @Mock
    private DocumentOrganizationFetcher documentOrganizationFetcher;
    
    
    private JavaSparkContext sparkContext;
    
    
    @Before
    public void setup() {
        
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName(getClass().getName());
        
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        conf.set("spark.driver.host", "localhost");
        
        sparkContext = new JavaSparkContext(conf);
        
    }
    
    @After
    public void cleanup() {
        
        if (sparkContext != null) {
            sparkContext.close();
        }
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void join() {
        
        // given
        
        JavaRDD<AffMatchAffiliation> affiliations = sparkContext.parallelize(ImmutableList.of(
                new AffMatchAffiliation("DOC1", 1),
                new AffMatchAffiliation("DOC1", 2),
                new AffMatchAffiliation("DOC2", 1), // document not present in documentOrganizations rdd
                new AffMatchAffiliation("DOC3", 1),
                new AffMatchAffiliation("DOC3", 2),
                new AffMatchAffiliation("DOC3", 3)));
        
        JavaRDD<AffMatchOrganization> organizations = sparkContext.parallelize(ImmutableList.of(
                new AffMatchOrganization("ORG1"), // organization not present in documentOrganizations rdd
                new AffMatchOrganization("ORG2"), 
                new AffMatchOrganization("ORG3"),
                new AffMatchOrganization("ORG4")));
        
        JavaRDD<AffMatchDocumentOrganization> documentOrganizations = sparkContext.parallelize(ImmutableList.of(
                new AffMatchDocumentOrganization("DOC1", "ORG2"),
                new AffMatchDocumentOrganization("DOC3", "ORG3"),
                new AffMatchDocumentOrganization("DOC3", "ORG4"),
                new AffMatchDocumentOrganization("DOC3", "ORG5"), // organization not present in organizations rdd
                new AffMatchDocumentOrganization("DOC4", "ORG2"))); // document not present in affiliations rdd
        
        when(documentOrganizationFetcher.fetchDocumentOrganizations()).thenReturn(documentOrganizations);
        
        
        // execute
        
        JavaRDD<Tuple2<AffMatchAffiliation, AffMatchOrganization>> affOrgPairsRDD = docOrgRelationAffOrgJoiner.join(affiliations, organizations);
        
        
        // assert
        
        List<Tuple2<AffMatchAffiliation, AffMatchOrganization>> affOrgPairs = affOrgPairsRDD.collect();
        
        List<Tuple2<AffMatchAffiliation, AffMatchOrganization>> expectedAffOrgPairs = ImmutableList.of(
                new Tuple2<>(new AffMatchAffiliation("DOC1", 1), new AffMatchOrganization("ORG2")),
                new Tuple2<>(new AffMatchAffiliation("DOC1", 2), new AffMatchOrganization("ORG2")),
                new Tuple2<>(new AffMatchAffiliation("DOC3", 1), new AffMatchOrganization("ORG3")),
                new Tuple2<>(new AffMatchAffiliation("DOC3", 2), new AffMatchOrganization("ORG3")),
                new Tuple2<>(new AffMatchAffiliation("DOC3", 3), new AffMatchOrganization("ORG3")),
                new Tuple2<>(new AffMatchAffiliation("DOC3", 1), new AffMatchOrganization("ORG4")),
                new Tuple2<>(new AffMatchAffiliation("DOC3", 2), new AffMatchOrganization("ORG4")),
                new Tuple2<>(new AffMatchAffiliation("DOC3", 3), new AffMatchOrganization("ORG4")));
        
        assertThat(affOrgPairs, containsInAnyOrder(expectedAffOrgPairs.toArray()));
    }
    
}
