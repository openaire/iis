package eu.dnetlib.iis.workflows.citationmatching.input;

import java.io.IOException;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import eu.dnetlib.iis.citationmatching.schemas.DocumentMetadata;
import eu.dnetlib.iis.common.utils.AvroAssertTestUtil;
import eu.dnetlib.iis.common.utils.JsonAvroTestUtils;

/**
 * @author madryk
 */
public class AuthorNameAttacherTest {

    private static final String DATA_DIRECTORY_PATH = "src/test/resources/eu/dnetlib/iis/workflows/citationmatching/data/input_transformer";
    
    
    private AuthorNameAttacher authorNameAttacher = new AuthorNameAttacher();
    
    private JavaSparkContext sparkContext;
    
    
    @Before
    public void before() {
        
        SparkConf conf = new SparkConf().setMaster("local").setAppName("AuthorNameAttacherTest")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
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
        
        JavaPairRDD<String, DocumentMetadata> documentsWithAuthorIds = sparkContext
                .parallelize(JsonAvroTestUtils.readJsonDataStore(documentwithAuthorIdPath, DocumentMetadata.class))
                .keyBy(x -> x.getId().toString());
        
        JavaPairRDD<String, Map<String, String>> documentAuthors = sparkContext.parallelizePairs(
                AuthorNameMappingDataProvider.fetchDocumentAuthors());
        
        
        
        // execute
        
        JavaPairRDD<String, DocumentMetadata> retDocuments = authorNameAttacher.attachAuthorNames(documentsWithAuthorIds, documentAuthors);
        
        
        // assert
        
        String expectedDocumentsPath = DATA_DIRECTORY_PATH + "/document.json";
        
        AvroAssertTestUtil.assertEqualsWithJsonIgnoreOrder(retDocuments.values().collect(), expectedDocumentsPath, DocumentMetadata.class);
    }

}
