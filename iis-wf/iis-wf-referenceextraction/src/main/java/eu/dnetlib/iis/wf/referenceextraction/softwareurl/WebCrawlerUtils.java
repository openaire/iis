package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import eu.dnetlib.iis.audit.schemas.Fault;
import eu.dnetlib.iis.common.fault.FaultUtils;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrl;
import eu.dnetlib.iis.wf.referenceextraction.ContentRetrieverResponse;
import scala.Tuple2;

/**
 * Web crawler utility methods.
 * @author mhorst
 *
 */
public class WebCrawlerUtils {

    /**
     * Obtains sources using content retriever.
     */
    public static Tuple2<JavaRDD<DocumentText>, JavaRDD<Fault>> obtainSources(JavaRDD<DocumentToSoftwareUrl> documentToSoftwareUrl,
            ContentRetriever contentRetriever, int numberOfPartitionsForCrawling) {
        JavaRDD<CharSequence> uniqueSoftwareUrl = documentToSoftwareUrl.map(e -> e.getSoftwareUrl()).distinct();
        
        JavaPairRDD<CharSequence, ContentRetrieverResponse> uniqueFilteredSoftwareUrlToSource = uniqueSoftwareUrl
                .repartition(numberOfPartitionsForCrawling)
                .mapToPair(e -> new Tuple2<CharSequence, ContentRetrieverResponse>(e, contentRetriever.retrieveUrlContent(e)));

        return new Tuple2<>(
                uniqueFilteredSoftwareUrlToSource.map(e -> DocumentText.newBuilder().setId(e._1).setText(e._2.getContent()).build()),
                uniqueFilteredSoftwareUrlToSource.filter(e -> e._2.getException() != null).map(e -> FaultUtils.exceptionToFault(e._1, e._2.getException(), null)));
    }
    
}
