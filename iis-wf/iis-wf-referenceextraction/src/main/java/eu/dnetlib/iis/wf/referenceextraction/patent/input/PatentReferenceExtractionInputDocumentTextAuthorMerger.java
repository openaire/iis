package eu.dnetlib.iis.wf.referenceextraction.patent.input;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.importer.schemas.Author;
import eu.dnetlib.iis.metadataextraction.schemas.DocumentText;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentTextWithAuthors;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class PatentReferenceExtractionInputDocumentTextAuthorMerger {
    private static final SparkAvroLoader sparkAvroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver sparkAvroSaver = new SparkAvroSaver();

    public static void main(String[] args) throws IOException {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPath);

            JavaRDD<DocumentText> documentTexts = sparkAvroLoader.loadJavaRDD(sc, params.inputDocumentTextPath, DocumentText.class);
            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> documentMetadatas =
                    sparkAvroLoader.loadJavaRDD(sc, params.inputExtractedDocumentMetadataMergedWithOriginalPath, ExtractedDocumentMetadataMergedWithOriginal.class);

            JavaPairRDD<CharSequence, DocumentText> documentTextsById = documentTexts
                    .mapToPair(x -> new Tuple2<>(x.getId(), x));

            JavaPairRDD<CharSequence, List<CharSequence>> documentMetadatasById = documentMetadatas
                    .mapToPair(x -> new Tuple2<>(x.getId(), filterAuthorsOrEmptyList(x.getImportedAuthors())));

            JavaRDD<DocumentTextWithAuthors> merged = documentTextsById
                    .join(documentMetadatasById)
                    .values()
                    .map(PatentReferenceExtractionInputDocumentTextAuthorMerger::merge);
            sparkAvroSaver.saveJavaRDD(merged, DocumentTextWithAuthors.SCHEMA$, params.outputPath);
        }
    }

    private static List<CharSequence> filterAuthorsOrEmptyList(List<Author> authors) {
        return Optional.ofNullable(authors)
                .map(x -> x.stream()
                        .map(Author::getFullname)
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
    }

    private static DocumentTextWithAuthors merge(Tuple2<DocumentText, List<CharSequence>> pair) {
        DocumentText documentText = pair._1();
        List<CharSequence> authors = pair._2();
        return DocumentTextWithAuthors.newBuilder()
                .setId(documentText.getId())
                .setText(documentText.getText())
                .setAuthors(authors)
                .build();
    }

    @Parameters(separators = "=")
    private static class JobParameters {
        @Parameter(names = "-inputDocumentTextPath", required = true)
        private String inputDocumentTextPath;

        @Parameter(names = "-inputExtractedDocumentMetadataMergedWithOriginalPath", required = true)
        private String inputExtractedDocumentMetadataMergedWithOriginalPath;

        @Parameter(names = "-outputPath", required = true)
        private String outputPath;
    }
}
