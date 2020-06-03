package eu.dnetlib.iis.wf.referenceextraction.patent.input;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Job responsible for filtering the input to the PatentMetadataRetrieverJob by
 * limiting the set of {@link ImportedPatent} records only to the ones matched
 * with publications.
 * 
 * @author mhorst
 *
 */
public class PatentMetadataRetrieverInputTransformerJob {
    
    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPath);

            JavaPairRDD<CharSequence, ImportedPatent> importedPatentsById = avroLoader
                    .loadJavaRDD(sc, params.inputImportedPatentPath, ImportedPatent.class)
                    .mapToPair(x -> new Tuple2<CharSequence, ImportedPatent>(x.getApplnNr(), x));

            JavaPairRDD<CharSequence, Boolean> matchedDedupedPatentsById = avroLoader
                    .loadJavaRDD(sc, params.inputMatchedPatentPath, DocumentToPatent.class).map(x -> x.getApplnNr())
                    .distinct().mapToPair(x -> new Tuple2<CharSequence, Boolean>(x, true));

            JavaRDD<ImportedPatent> filteredPatents = matchedDedupedPatentsById.join(importedPatentsById).values().map(x -> x._2);
            
            avroSaver.saveJavaRDD(filteredPatents, ImportedPatent.SCHEMA$, params.outputPath);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class JobParameters {
        @Parameter(names = "-inputImportedPatentPath", required = true)
        private String inputImportedPatentPath;
        
        @Parameter(names = "-inputMatchedPatentPath", required = true)
        private String inputMatchedPatentPath;

        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
    }
}
