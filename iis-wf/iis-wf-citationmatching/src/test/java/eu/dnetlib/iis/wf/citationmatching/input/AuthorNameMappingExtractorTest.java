package eu.dnetlib.iis.wf.citationmatching.input;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;
import eu.dnetlib.iis.importer.schemas.Person;

/**
 * @author madryk
 */
public class AuthorNameMappingExtractorTest {

    private static final String DATA_DIRECTORY_PATH = "src/test/resources/eu/dnetlib/iis/wf/citationmatching/data/input_transformer";
    
    
    private AuthorNameMappingExtractor authorNameMappingExtractor = new AuthorNameMappingExtractor();
    
    private JavaSparkContext sparkContext;
    
    
    @Before
    public void before() {
        
        SparkConf conf = new SparkConf().setMaster("local").setAppName("AuthorNameAttacherTest")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        conf.set("spark.driver.host", "localhost");
        
        sparkContext = new JavaSparkContext(conf);
        
    }
    
    
    @After
    public void after() {
        
        if (sparkContext != null) {
            sparkContext.close();
            sparkContext = null;
        }
        
    }
    
    
    //------------------------ TESTS --------------------------
    
    @Test
    public void attachAuthorNames() throws IOException {
        
        
        // given
        
        String documentwithAuthorIdPath = DATA_DIRECTORY_PATH + "/document_without_author_name.json";
        String personPath = DATA_DIRECTORY_PATH + "/person.json";
        
        JavaPairRDD<String, DocumentMetadata> documentsWithAuthorIds = sparkContext
                .parallelize(JsonAvroTestUtils.readJsonDataStore(documentwithAuthorIdPath, DocumentMetadata.class))
                .keyBy(x -> x.getId().toString());
        
        JavaRDD<Person> persons = sparkContext.parallelize(JsonAvroTestUtils.readJsonDataStore(personPath, Person.class));
        
        
        // execute
        
        JavaPairRDD<String, Map<String, String>> retAuthorNameMapping = authorNameMappingExtractor.extractAuthorNameMapping(documentsWithAuthorIds, persons);
        
        
        // assert
        
        assertThat(retAuthorNameMapping.collect(), containsInAnyOrder(AuthorNameMappingDataProvider.fetchDocumentAuthors().toArray()));
    }

}
