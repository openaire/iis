package eu.dnetlib.iis.wf.transformers.ingest.pmc.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark job that converts PMC-extracted document metadata into the common
 * metadataextraction schema.
 */
public class PmcMetadataTransformerJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.output);

            JavaRDD<eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata> input =
                    avroLoader.loadJavaRDD(sc, params.input,
                            eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata.class);

            JavaRDD<eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata> output =
                    input.map(PmcMetadataTransformerJob::convert);

            avroSaver.saveJavaRDD(output,
                    eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata.SCHEMA$,
                    params.output);
        }
    }

    //------------------------ PRIVATE --------------------------

    private static eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata convert(
            eu.dnetlib.iis.ingest.pmc.metadata.schemas.ExtractedDocumentMetadata src) {
        return eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata.newBuilder()
                .setId(src.getId())
                .setTitle(null)
                .setAbstract$(null)
                .setLanguage(null)
                .setKeywords(null)
                .setExternalIdentifiers(src.getExternalIdentifiers())
                .setJournal(src.getJournal())
                .setYear(null)
                .setPublisher(null)
                .setReferences(convertReferences(src.getReferences()))
                .setAuthors(convertAuthors(src.getAuthors()))
                .setAffiliations(convertAffiliations(src.getAffiliations()))
                .setVolume(null)
                .setIssue(null)
                .setPages(convertRange(src.getPages()))
                .setPublicationTypeName(src.getEntityType())
                .setText(src.getText())
                .setExtractedBy(null)
                .build();
    }

    private static List<eu.dnetlib.iis.metadataextraction.schemas.Author> convertAuthors(
            List<eu.dnetlib.iis.ingest.pmc.metadata.schemas.Author> srcAuthors) {
        if (srcAuthors == null) {
            return null;
        }
        List<eu.dnetlib.iis.metadataextraction.schemas.Author> result = new ArrayList<>();
        for (eu.dnetlib.iis.ingest.pmc.metadata.schemas.Author a : srcAuthors) {
            result.add(eu.dnetlib.iis.metadataextraction.schemas.Author.newBuilder()
                    .setAuthorFullName(a.getFullname())
                    .setAffiliationPositions(a.getAffiliationPositions())
                    .build());
        }
        return result;
    }

    private static List<eu.dnetlib.iis.metadataextraction.schemas.Affiliation> convertAffiliations(
            List<eu.dnetlib.iis.ingest.pmc.metadata.schemas.Affiliation> srcAffiliations) {
        if (srcAffiliations == null) {
            return null;
        }
        List<eu.dnetlib.iis.metadataextraction.schemas.Affiliation> result = new ArrayList<>();
        for (eu.dnetlib.iis.ingest.pmc.metadata.schemas.Affiliation a : srcAffiliations) {
            result.add(eu.dnetlib.iis.metadataextraction.schemas.Affiliation.newBuilder()
                    .setOrganization(a.getOrganization())
                    .setCountryName(a.getCountryName())
                    .setCountryCode(a.getCountryCode())
                    .setAddress(a.getAddress())
                    .setRawText(a.getRawText())
                    .build());
        }
        return result;
    }

    private static eu.dnetlib.iis.metadataextraction.schemas.Range convertRange(
            eu.dnetlib.iis.ingest.pmc.metadata.schemas.Range src) {
        if (src == null) {
            return null;
        }
        return eu.dnetlib.iis.metadataextraction.schemas.Range.newBuilder()
                .setStart(src.getStart())
                .setEnd(src.getEnd())
                .build();
    }

    private static List<eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata> convertReferences(
            List<eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceMetadata> srcRefs) {
        if (srcRefs == null) {
            return null;
        }
        List<eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata> result = new ArrayList<>();
        for (eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceMetadata ref : srcRefs) {
            result.add(eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata.newBuilder()
                    .setBasicMetadata(convertReferenceBasicMetadata(ref.getBasicMetadata()))
                    .setPosition(ref.getPosition())
                    .setText(ref.getText())
                    .build());
        }
        return result;
    }

    private static eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata convertReferenceBasicMetadata(
            eu.dnetlib.iis.ingest.pmc.metadata.schemas.ReferenceBasicMetadata src) {
        return eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata.newBuilder()
                .setTitle(src.getTitle())
                .setAuthors(src.getAuthors())
                .setPages(convertRange(src.getPages()))
                .setSource(src.getSource())
                .setVolume(src.getVolume())
                .setYear(src.getYear())
                .setEdition(null)
                .setPublisher(null)
                .setLocation(null)
                .setSeries(null)
                .setIssue(src.getIssue())
                .setUrl(null)
                .setExternalIds(src.getExternalIds())
                .build();
    }

    @Parameters(separators = "=")
    private static class Params {

        @Parameter(names = "-input", required = true)
        private String input;

        @Parameter(names = "-output", required = true)
        private String output;
    }
}
