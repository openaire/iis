package eu.dnetlib.iis.workflows.citationmatching.direct.udfs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;

/**
 * Deduplicates bag of tuples where tuple[0] is oaid, tuple[1] is pmid document type.
 * 'research-article' type has precedence over any other type when more than one entry provided.
 * Identifiers are sorted lexicographically.
 *
 * @author mhorst
 */
public class DeduplicateIdsWithDocumentType extends EvalFunc<DataBag> {

	public static final String DOCUMENT_TYPE_RESEARCH_ARTICLE = "research-article";
	
    @Override
    public DataBag exec(Tuple tuple) throws IOException {
        if (tuple == null || tuple.size() == 0) {
            return null;
        }
        DataBag db = (DataBag) tuple.get(0);
        if (db==null) {
        	return null;
        }
    	int count = 0;
		Tuple firstTuple = null;
		Iterator<Tuple> it = db.iterator();
        while (it.hasNext()) {
        	Tuple currentTuple = it.next();
        	if (count==0) {
        		firstTuple = currentTuple;
        	}
        	if (DOCUMENT_TYPE_RESEARCH_ARTICLE.equals(currentTuple.get(1))) {
        		return BagFactory.getInstance().newDefaultBag(
        				Lists.<Tuple>newArrayList(currentTuple));
        	}
        	count++;
        }
		if (firstTuple!=null) {
			return BagFactory.getInstance().newDefaultBag(
    				Lists.<Tuple>newArrayList(firstTuple));
		}
//		fallback
		return null;
    }

    @Override
    public Schema outputSchema(Schema input) {
    	return input;
    }
    
}
