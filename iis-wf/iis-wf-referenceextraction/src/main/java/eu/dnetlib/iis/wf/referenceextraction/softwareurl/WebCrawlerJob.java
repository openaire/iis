package eu.dnetlib.iis.wf.referenceextraction.softwareurl;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrl;
import eu.dnetlib.iis.referenceextraction.softwareurl.schemas.DocumentToSoftwareUrlWithSource;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;


/**
 * Retrieves HTML pages pointed by softwareURL field from {@link DocumentToSoftwareUrl}.
 * For each {@link DocumentToSoftwareUrl} input record builds {@link DocumentToSoftwareUrlWithSource} if the page was available.
 * @author mhorst
 *
 */
public class WebCrawlerJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws Exception {
        
        WebCrawlerJobParameters params = new WebCrawlerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        final int connectionTimeout = params.connectionTimeout;
        final int readTimeout = params.readTimeout;
        final int maxPageContentLength = params.maxPageContentLength;
        
        @SuppressWarnings("unchecked")
        Class<ContentRetriever> clazz = (Class<ContentRetriever>) Class.forName(params.contentRetrieverClassName);
        ContentRetriever contentRetriever = clazz.getConstructor().newInstance();
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);

            JavaRDD<DocumentToSoftwareUrl> documentToSoftwareUrl = avroLoader.loadJavaRDD(sc, params.inputAvroPath,
                    DocumentToSoftwareUrl.class);

            JavaRDD<CharSequence> uniqueSoftwareUrl = documentToSoftwareUrl.map(e -> e.getSoftwareUrl()).distinct();
            
            JavaPairRDD<CharSequence, CharSequence> uniqueFilteredSoftwareUrlToSource = uniqueSoftwareUrl
                    .mapToPair(e -> new Tuple2<CharSequence, CharSequence>(e, contentRetriever.retrieveUrlContent(e, 
                            connectionTimeout, readTimeout, maxPageContentLength)));
            
            JavaPairRDD<CharSequence, DocumentToSoftwareUrl> softwareUrlToFullObject = documentToSoftwareUrl
                    .mapToPair(e -> new Tuple2<CharSequence, DocumentToSoftwareUrl>(e.getSoftwareUrl(), e));
            
            JavaRDD<Tuple2<DocumentToSoftwareUrl, CharSequence>> softwareUrlJoinedWithSource = softwareUrlToFullObject
                    .join(uniqueFilteredSoftwareUrlToSource).values();
            
            JavaRDD<DocumentToSoftwareUrlWithSource> result = softwareUrlJoinedWithSource
                    .map(e -> buildDocumentToSoftwareUrlWithSource(e));

            avroSaver.saveJavaRDD(result, DocumentToSoftwareUrlWithSource.SCHEMA$, params.outputAvroPath);
        }
        
    }
    
    //------------------------ PRIVATE --------------------------
    
    private static DocumentToSoftwareUrlWithSource buildDocumentToSoftwareUrlWithSource(Tuple2<DocumentToSoftwareUrl,CharSequence> source) {
        DocumentToSoftwareUrlWithSource.Builder resultBuilder = DocumentToSoftwareUrlWithSource.newBuilder();
        resultBuilder.setDocumentId(source._1.getDocumentId());
        resultBuilder.setSoftwareUrl(source._1.getSoftwareUrl());
        resultBuilder.setRepositoryName(source._1.getRepositoryName());
        resultBuilder.setSource(source._2);
        return resultBuilder.build();
    }
    
    @Parameters(separators = "=")
    private static class WebCrawlerJobParameters {
        
        @Parameter(names = "-inputAvroPath", required = true)
        private String inputAvroPath;
        
        @Parameter(names = "-contentRetrieverClassName", required = true)
        private String contentRetrieverClassName;
        
        @Parameter(names = "-connectionTimeout", required = true)
        private int connectionTimeout;
        
        @Parameter(names = "-readTimeout", required = true)
        private int readTimeout;
        
        @Parameter(names = "-maxPageContentLength", required = true)
        private int maxPageContentLength;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;

    }
}
