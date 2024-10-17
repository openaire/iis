package pl.edu.icm.coansys.citations;

import org.apache.spark.api.java.JavaPairRDD;

import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

/**
 * Attacher of documents in place of document id
 * in matched citations
 * 
 * @author madryk
 */
public class DocumentAttacher {

	public JavaPairRDD<String, MatchableEntity> attachDocuments(JavaPairRDD<String, String> matchedCitations, JavaPairRDD<String, MatchableEntity> documents) {
		
		return matchedCitations
				.mapToPair(x -> x.swap())
				.join(documents)
				.mapToPair(x -> new Tuple2<String, MatchableEntity>(x._2._1, x._2._2));
		
	}
}
