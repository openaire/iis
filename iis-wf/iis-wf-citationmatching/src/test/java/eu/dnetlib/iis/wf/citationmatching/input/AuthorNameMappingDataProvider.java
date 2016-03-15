package eu.dnetlib.iis.wf.citationmatching.input;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import scala.Tuple2;

/**
 * @author madryk
 */
class AuthorNameMappingDataProvider {

    //------------------------ CONSTRUCTORS --------------------------
    
    private AuthorNameMappingDataProvider() { }
    
    
    //------------------------ LOGIC --------------------------
    
    public static List<Tuple2<String, Map<String, String>>> fetchDocumentAuthors() {
        
        Map<String, String> doc1AuthorMapping = Maps.newHashMap();
        doc1AuthorMapping.put("author-id-1", "Orson Scott Card");
        doc1AuthorMapping.put("author-id-2", "Ender");
        doc1AuthorMapping.put("author-id-NOT_EXISTING", "_UNDEFINED_");
        
        Map<String, String> doc2AuthorMapping = Maps.newHashMap();
        doc2AuthorMapping.put("author-id-1", "Orson Scott Card");
        doc2AuthorMapping.put("author-id-3", "Alvin Maker");
        
        Map<String, String> doc3AuthorMapping = Maps.newHashMap();
        doc3AuthorMapping.put("author-id-NOT_EXISTING_2", "_UNDEFINED_");
        
//        Map<String, String> doc4AuthorMapping = Maps.newHashMap();
        
        Map<String, String> doc99AuthorMapping = Maps.newHashMap();
        doc99AuthorMapping.put("author-id-1", "Orson Scott Card");
        doc99AuthorMapping.put("author-id-2", "Ender");
        doc99AuthorMapping.put("author-id-3", "Alvin Maker");
        doc99AuthorMapping.put("author-id-4", "Terry Pratchett");
        doc99AuthorMapping.put("author-id-199", "An Orson Scott Card");
        doc99AuthorMapping.put("author-id-299", "Ender Speaker");
        doc99AuthorMapping.put("author-id-399", "CAlvin Maker");
        doc99AuthorMapping.put("author-id-499", "Terry Williams");
        
        List<Tuple2<String, Map<String, String>>> documentAuthorNameMapping = Lists.newArrayList();
        documentAuthorNameMapping.add(new Tuple2<>("id-1", doc1AuthorMapping));
        documentAuthorNameMapping.add(new Tuple2<>("id-2", doc2AuthorMapping));
        documentAuthorNameMapping.add(new Tuple2<>("id-3", doc3AuthorMapping));
//        documentAuthorNameMapping.add(new Tuple2<>("id-4", doc4AuthorMapping));
        documentAuthorNameMapping.add(new Tuple2<>("id-99", doc99AuthorMapping));
        
        return documentAuthorNameMapping;
    }
}
