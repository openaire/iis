package eu.dnetlib.iis.wf.transformers.metadataextraction.skip_extracted;

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
 * Spark job that produces two filtered datasets:
 * <ul>
 *   <li>outputDocumentContent: DocumentContentUrl records whose id is NOT present in the
 *       input metadata (i.e. not yet extracted).</li>
 *   <li>outputDocumentMeta: ExtractedDocumentMetadata records whose publicationTypeName is
 *       null or not "EMPTY", and whose id IS present in the input document content.</li>
 * </ul>
 *
 * Replaces the PIG-based skip_extracted transformer workflow step.
 */
public class SkipExtractedJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputDocumentContent);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputDocumentMeta);

            JavaRDD<DocumentContentUrl> documentContent = avroLoader.loadJavaRDD(sc,
                    params.inputDocumentContent, DocumentContentUrl.class);
            JavaRDD<ExtractedDocumentMetadata> documentMeta = avroLoader.loadJavaRDD(sc,
                    params.inputDocumentMeta, ExtractedDocumentMetadata.class);

            JavaPairRDD<String, DocumentContentUrl> contentById = documentContent.mapToPair(
                    d -> new Tuple2<>(d.getId().toString(), d));
            JavaPairRDD<String, ExtractedDocumentMetadata> metaById = documentMeta.mapToPair(
                    m -> new Tuple2<>(m.getId().toString(), m));

            // output_content: content records with no matching meta record
            JavaRDD<DocumentContentUrl> outputContent = contentById
                    .leftOuterJoin(metaById)
                    .filter(pair -> !pair._2._2.isPresent())
                    .map(pair -> pair._2._1);

            // output_meta: meta records whose publicationTypeName is null or not "EMPTY",
            //              intersected with content ids
            JavaRDD<ExtractedDocumentMetadata> filteredMeta = documentMeta.filter(m -> {
                CharSequence pubType = m.getPublicationTypeName();
                return pubType == null || !"EMPTY".equals(pubType.toString());
            });
            JavaPairRDD<String, ExtractedDocumentMetadata> filteredMetaById = filteredMeta.mapToPair(
                    m -> new Tuple2<>(m.getId().toString(), m));
            JavaRDD<ExtractedDocumentMetadata> outputMeta = filteredMetaById
                    .join(contentById)
                    .map(pair -> pair._2._1);

            avroSaver.saveJavaRDD(outputContent, DocumentContentUrl.SCHEMA$, params.outputDocumentContent);
            avroSaver.saveJavaRDD(outputMeta, ExtractedDocumentMetadata.SCHEMA$, params.outputDocumentMeta);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-inputDocumentContent", required = true)
        private String inputDocumentContent;

        @Parameter(names = "-inputDocumentMeta", required = true)
        private String inputDocumentMeta;

        @Parameter(names = "-outputDocumentContent", required = true)
        private String outputDocumentContent;

        @Parameter(names = "-outputDocumentMeta", required = true)
        private String outputDocumentMeta;
    }
}
