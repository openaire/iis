package eu.dnetlib.iis.wf.referenceextraction.patent.input;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.PatentReferenceExtractionInput;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

public class PatentReferenceExtractionInputTransformerJob {
    private static final SparkAvroLoader sparkAvroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver sparkAvroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaRDD<PatentReferenceExtractionInput> convertedRDD = sparkAvroLoader.loadJavaRDD(sc, params.inputPath, Patent.class)
                    .map(PatentReferenceExtractionInputTransformerJob::convert);
            sparkAvroSaver.saveJavaRDD(convertedRDD, PatentReferenceExtractionInput.SCHEMA$, params.outputPath);
        }
    }

    //------------------------ PRIVATE --------------------------

    private static PatentReferenceExtractionInput convert(Patent patent) {
        return PatentReferenceExtractionInput.newBuilder()
                .setApplnId(patent.getApplnId())
                .setApplnAuth(patent.getApplnAuth())
                .setApplnNr(patent.getApplnNr())
                .setApplnNrEpodoc(patent.getApplnNrEpodoc())
                .build();
    }

    @Parameters(separators = "=")
    private static class JobParameters {
        @Parameter(names = "-inputPath", required = true)
        private String inputPath;

        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
    }
}
