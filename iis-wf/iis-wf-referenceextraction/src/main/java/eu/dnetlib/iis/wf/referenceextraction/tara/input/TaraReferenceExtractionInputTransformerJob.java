package eu.dnetlib.iis.wf.referenceextraction.tara.input;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Optional;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.tara.schemas.DocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * 
 * @author mhorst
 *
 */
public class TaraReferenceExtractionInputTransformerJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws InterruptedException, IOException {
        
        TaraReferenceExtractionInputTransformerJobParameters params = new TaraReferenceExtractionInputTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> inputMeta = avroLoader.loadJavaRDD(sc, params.inputMetadata, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            JavaRDD<DocumentText> inputText = avroLoader.loadJavaRDD(sc, params.inputText, DocumentText.class);
            
            JavaPairRDD<CharSequence, Tuple2<CharSequence, CharSequence>> idToTitleAndAbstract = inputMeta.mapToPair(x -> new Tuple2<>(x.getId(), new Tuple2<>(x.getTitle(), x.getAbstract$()))); 
            JavaPairRDD<CharSequence, CharSequence> idToText = inputText.mapToPair(x -> new Tuple2<>(x.getId(), x.getText()));
            JavaPairRDD<CharSequence, Tuple2<Tuple2<CharSequence, CharSequence>, Optional<CharSequence>>> joined = idToTitleAndAbstract.leftOuterJoin(idToText);
            
            JavaRDD<DocumentMetadata> output = joined.map(x -> buildMetadata(x._1, x._2));

            avroSaver.saveJavaRDD(output, DocumentMetadata.SCHEMA$, params.output);
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private static DocumentMetadata buildMetadata(CharSequence id, Tuple2<Tuple2<CharSequence, CharSequence>, Optional<CharSequence>> rddRecord) {
        DocumentMetadata.Builder metaBuilder = DocumentMetadata.newBuilder();
        metaBuilder.setId(id);
        metaBuilder.setTitle(rddRecord._1._1);
        metaBuilder.setAbstract$(rddRecord._1._2);
        if (rddRecord._2.isPresent()) {
            metaBuilder.setText(rddRecord._2.get());
        }
        return metaBuilder.build();
    }
    
    @Parameters(separators = "=")
    private static class TaraReferenceExtractionInputTransformerJobParameters {
        
        @Parameter(names = "-inputMetadata", required = true)
        private String inputMetadata;
        
        @Parameter(names = "-inputText", required = true)
        private String inputText;
        
        @Parameter(names = "-output", required = true)
        private String output;
        
    }
}
