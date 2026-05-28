package eu.dnetlib.iis.wf.transformers.metadataextraction.checksum.postprocessing.meta;

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
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Spark job that remaps the id in ExtractedDocumentMetadata records from a contentChecksum
 * back to the original document id. Only DocumentContentUrl records with a non-null
 * contentChecksum are considered. After checksum preprocessing, the metadata id equals the
 * checksum, so the join is metadata.id = dcu.contentChecksum → replace metadata.id with dcu.id.
 */
public class ChecksumMetaPostprocessingJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);

            JavaRDD<ExtractedDocumentMetadata> meta = avroLoader.loadJavaRDD(sc,
                    params.inputExtractedDocumentMetadata, ExtractedDocumentMetadata.class);
            JavaRDD<DocumentContentUrl> dcu = avroLoader.loadJavaRDD(sc,
                    params.inputDocumentContentUrl, DocumentContentUrl.class);

            // Build a mapping: checksum -> original document id
            // After preprocessing, dcu.id IS the checksum, so we use dcu.contentChecksum
            // but to join with meta.id we use dcu.contentChecksum as the key
            JavaPairRDD<String, String> checksumToDocId = dcu
                    .filter(d -> d.getContentChecksum() != null)
                    .mapToPair(d -> new Tuple2<>(d.getContentChecksum().toString(), d.getId().toString()));

            // Key metadata by id (which equals checksum after preprocessing)
            JavaPairRDD<String, ExtractedDocumentMetadata> metaById = meta.mapToPair(
                    m -> new Tuple2<>(m.getId().toString(), m));

            // Inner join on checksum: replace metadata id with the original document id
            JavaRDD<ExtractedDocumentMetadata> output = checksumToDocId
                    .join(metaById)
                    .map(pair -> ExtractedDocumentMetadata.newBuilder(pair._2._2)
                            .setId(pair._2._1)
                            .build());

            avroSaver.saveJavaRDD(output, ExtractedDocumentMetadata.SCHEMA$, params.output);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-inputExtractedDocumentMetadata", required = true)
        private String inputExtractedDocumentMetadata;

        @Parameter(names = "-inputDocumentContentUrl", required = true)
        private String inputDocumentContentUrl;

        @Parameter(names = "-output", required = true)
        private String output;
    }
}
