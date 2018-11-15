package eu.dnetlib.iis.wf.affmatching;

import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganization;
import eu.dnetlib.iis.wf.affmatching.model.MatchedOrganizationWithProvenance;
import eu.dnetlib.iis.wf.affmatching.write.AffMatchDedupReportGenerator;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Job deduplicating {@link MatchedOrganization} records and assigning provenance into {@link MatchedOrganizationWithProvenance} deduplicated output records.
 * 
 * @author mhorst
 */

public class AffMatchingDedupJob {
    
    
    //------------------------ LOGIC --------------------------
    
    public static void main(String[] args) throws IOException {
        
    	ProjectRelatedDocOrgMatchingJobParameters params = new ProjectRelatedDocOrgMatchingJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);
    
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");
    
        SparkAvroLoader avroLoader = new SparkAvroLoader();
        SparkAvroSaver sparkAvroSaver = new SparkAvroSaver();
        AffMatchDedupReportGenerator reportGenerator = new AffMatchDedupReportGenerator();
        
        final String inputAPath = params.inputAPath;
        final String inputBPath = params.inputBPath;
        final String inferenceProvenanceInputA = params.inferenceProvenanceInputA;
        final String inferenceProvenanceInputB = params.inferenceProvenanceInputB;
        
        final String outputPath = params.outputAvroPath;
        final String outputReportPath = params.outputAvroReportPath;
        
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputAvroReportPath);
            
            JavaRDD<MatchedOrganization> inputA = avroLoader.loadJavaRDD(sc, inputAPath, MatchedOrganization.class);
            JavaRDD<MatchedOrganization> inputB = avroLoader.loadJavaRDD(sc, inputBPath, MatchedOrganization.class);

            JavaPairRDD<String, Tuple2<MatchedOrganization,String>> inputAPair = inputA.mapToPair(x -> new Tuple2<>(generateKey(x), new Tuple2<>(x, inferenceProvenanceInputA)));
            
            JavaPairRDD<String, Tuple2<MatchedOrganization,String>> inputBPair = inputB.mapToPair(x -> new Tuple2<>(generateKey(x), new Tuple2<>(x, inferenceProvenanceInputB)));
            
            JavaPairRDD<String, Tuple2<MatchedOrganization,String>> unionedPair = inputAPair.union(inputBPair);
            
            JavaRDD<MatchedOrganizationWithProvenance> results = unionedPair.reduceByKey((x, y) -> x._1.getMatchStrength() >= y._1.getMatchStrength() ? x : y).map(x -> buildMatchedOrganizationWithProvenance(x._2));
            
            results.cache();
            
            sparkAvroSaver.saveJavaRDD(results, MatchedOrganizationWithProvenance.SCHEMA$, outputPath);
            
            List<ReportEntry> reportEntries = reportGenerator.generateReport(results);
            
            sparkAvroSaver.saveJavaRDD(sc.parallelize(reportEntries), ReportEntry.SCHEMA$, outputReportPath);

            }
    }
    
    
    //------------------------ PRIVATE --------------------------
    
    private static String generateKey(MatchedOrganization source) {
        StringBuffer strBuff = new StringBuffer();
        strBuff.append(source.getDocumentId());
        strBuff.append('|');
        strBuff.append(source.getOrganizationId());
        return strBuff.toString();
    }
    
    private static MatchedOrganizationWithProvenance buildMatchedOrganizationWithProvenance(Tuple2<MatchedOrganization,String> source) {
        MatchedOrganizationWithProvenance.Builder builder = MatchedOrganizationWithProvenance.newBuilder();
        builder.setDocumentId(source._1.getDocumentId());
        builder.setOrganizationId(source._1.getOrganizationId());
        builder.setMatchStrength(source._1.getMatchStrength());
        builder.setProvenance(source._2);
        return builder.build();
    }
    
    @Parameters(separators = "=")
    private static class ProjectRelatedDocOrgMatchingJobParameters {
        
        @Parameter(names = "-inputAPath", required = true, description="path to the first datastore to be merged and deduplicated")
        private String inputAPath;
        
        @Parameter(names = "-inputBPath", required = true, description="path to the second datastore to be merged and deduplicated")
        private String inputBPath;
        
        @Parameter(names = "-inferenceProvenanceInputA", required = true, description="inference provenance to be defined for the first input")
        private String inferenceProvenanceInputA;
        
        @Parameter(names = "-inferenceProvenanceInputB", required = true, description="inference provenance to be defined for the second input")
        private String inferenceProvenanceInputB;
        
        @Parameter(names = "-outputAvroPath", required = true)
        private String outputAvroPath;
        
        @Parameter(names = "-outputAvroReportPath", required = true, description="path to a directory with the execution result report")
        private String outputAvroReportPath;
        
    }
    
}
