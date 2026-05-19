package eu.dnetlib.iis.wf.transformers.common.citations.from.referencemetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.citations.schemas.Citation;
import eu.dnetlib.iis.common.citations.schemas.CitationEntry;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark job that extracts flat Citation records from the references embedded in
 * ExtractedDocumentMetadataMergedWithOriginal documents.
 *
 * For each reference in each document a Citation is produced with a CitationEntry
 * holding the position, raw text, null destination and the reference's external IDs.
 *
 * Replaces the PIG-based citations/from/referencemetadata transformer workflow step.
 */
public class CitationsFromReferenceMetadataTransformerJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);

            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> input = avroLoader.loadJavaRDD(sc,
                    params.input, ExtractedDocumentMetadataMergedWithOriginal.class);

            JavaRDD<Citation> output = input.flatMap(doc -> {
                List<ReferenceMetadata> references = doc.getReferences();
                if (references == null) {
                    return Collections.emptyIterator();
                }
                List<Citation> citations = new ArrayList<>();
                for (ReferenceMetadata ref : references) {
                    Map<CharSequence, CharSequence> externalIds = null;
                    ReferenceBasicMetadata basicMetadata = ref.getBasicMetadata();
                    if (basicMetadata != null && basicMetadata.getExternalIds() != null) {
                        externalIds = basicMetadata.getExternalIds();
                    }
                    CitationEntry entry = CitationEntry.newBuilder()
                            .setPosition(ref.getPosition() != null ? ref.getPosition() : 0)
                            .setRawText(ref.getText())
                            .setDestinationDocumentId(null)
                            .setConfidenceLevel(null)
                            .setExternalDestinationDocumentIds(externalIds)
                            .build();
                    citations.add(Citation.newBuilder()
                            .setSourceDocumentId(doc.getId())
                            .setEntry(entry)
                            .build());
                }
                return citations.iterator();
            });

            avroSaver.saveJavaRDD(output, Citation.SCHEMA$, params.output);
        }
    }

    //------------------------ PRIVATE --------------------------

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-input", required = true)
        private String input;

        @Parameter(names = "-output", required = true)
        private String output;
    }
}
