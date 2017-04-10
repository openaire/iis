package eu.dnetlib.iis.wf.affmatching.bucket.projectorg.read;

import static com.google.common.collect.ImmutableList.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.dnetlib.iis.wf.affmatching.bucket.projectorg.model.AffMatchDocumentProject;

/**
 * @author madryk
 */
public class DocumentProjectMergerTest {

    private DocumentProjectMerger documentProjectMerger = new DocumentProjectMerger();
    
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
    public void merge() {
        
        // given
        
        JavaRDD<AffMatchDocumentProject> firstDocumentProjects = sparkContext.parallelize(of(
                new AffMatchDocumentProject("DOC1", "PROJ1", 1f),
                new AffMatchDocumentProject("DOC1", "PROJ2", 0.6f),
                new AffMatchDocumentProject("DOC1", "PROJ3", 0.4f),
                new AffMatchDocumentProject("DOC2", "PROJ4", 0.8f)));
        
        JavaRDD<AffMatchDocumentProject> secondDocumentProjects = sparkContext.parallelize(of(
                new AffMatchDocumentProject("DOC1", "PROJ1", 0.3f),
                new AffMatchDocumentProject("DOC1", "PROJ2", 1f),
                new AffMatchDocumentProject("DOC1", "PROJ4", 0.7f)));
        
        
        // execute
        
        JavaRDD<AffMatchDocumentProject> retDocumentProjects = documentProjectMerger
                .merge(firstDocumentProjects, secondDocumentProjects);
        
        
        // assert
        
        List<AffMatchDocumentProject> expectedDocumentProjects = of(
                new AffMatchDocumentProject("DOC1", "PROJ1", 1f), // from first rdd (higher confidence level)
                new AffMatchDocumentProject("DOC1", "PROJ2", 1f), // from second rdd (higher confidence level)
                new AffMatchDocumentProject("DOC1", "PROJ3", 0.4f),
                new AffMatchDocumentProject("DOC1", "PROJ4", 0.7f),
                new AffMatchDocumentProject("DOC2", "PROJ4", 0.8f));
        
        assertThat(retDocumentProjects.collect(), containsInAnyOrder(expectedDocumentProjects.toArray()));
        
    }
}
