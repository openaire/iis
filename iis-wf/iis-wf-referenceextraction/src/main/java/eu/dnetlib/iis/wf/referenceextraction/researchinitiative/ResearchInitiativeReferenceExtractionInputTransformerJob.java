package eu.dnetlib.iis.wf.referenceextraction.researchinitiative;

import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Optional;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.importer.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.DocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;
import scala.Tuple4;

/**
 * 
 * @author mhorst
 *
 */
public class ResearchInitiativeReferenceExtractionInputTransformerJob {
    
    private static SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static SparkAvroSaver avroSaver = new SparkAvroSaver();
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws InterruptedException, IOException {
        
    	ResearchInitiativeReferenceExtractionInputTransformerJobParameters params = new ResearchInitiativeReferenceExtractionInputTransformerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
        
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);
            
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> inputMeta = avroLoader.loadJavaRDD(sc, params.inputMetadata, ExtractedDocumentMetadataMergedWithOriginal.class);
            
            JavaRDD<DocumentText> inputText = avroLoader.loadJavaRDD(sc, params.inputText, DocumentText.class);
            
            JavaPairRDD<CharSequence, Tuple4<CharSequence, Integer, List<Author>, List<CharSequence>>> idToMeta = inputMeta.mapToPair(x -> new Tuple2<>(x.getId(), 
            		new Tuple4<>(x.getTitle(), x.getYear(), x.getImportedAuthors(), x.getExtractedAuthorFullNames()))); 
            JavaPairRDD<CharSequence, CharSequence> idToText = inputText.mapToPair(x -> new Tuple2<>(x.getId(), x.getText()));
            JavaPairRDD<CharSequence, Tuple2<Tuple4<CharSequence, Integer, List<Author>, List<CharSequence>>, Optional<CharSequence>>> joined = idToMeta.leftOuterJoin(idToText);
            
            JavaRDD<DocumentMetadata> output = joined.map(x -> buildMetadata(x._1, x._2));

            avroSaver.saveJavaRDD(output, DocumentMetadata.SCHEMA$, params.output);
        }
        
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private static DocumentMetadata buildMetadata(CharSequence id, Tuple2<Tuple4<CharSequence, Integer, List<Author>, List<CharSequence>>, Optional<CharSequence>> rddRecord) {
        DocumentMetadata.Builder metaBuilder = DocumentMetadata.newBuilder();
        metaBuilder.setId(id);
        metaBuilder.setTitle(rddRecord._1._1());
        
        if (rddRecord._1._2() != null) {
        	metaBuilder.setYear(String.valueOf(rddRecord._1._2()));	
        }
        
        metaBuilder.setImportedAuthors(rddRecord._1._3());
        metaBuilder.setExtractedAuthorFullNames(rddRecord._1._4());
        
        if (rddRecord._2.isPresent()) {
        	metaBuilder.setText(rddRecord._2.get());
        }
        
        return metaBuilder.build();
    }
    
    @Parameters(separators = "=")
    private static class ResearchInitiativeReferenceExtractionInputTransformerJobParameters {
        
        @Parameter(names = "-inputMetadata", required = true)
        private String inputMetadata;
        
        @Parameter(names = "-inputText", required = true)
        private String inputText;
        
        @Parameter(names = "-output", required = true)
        private String output;
        
    }
}
