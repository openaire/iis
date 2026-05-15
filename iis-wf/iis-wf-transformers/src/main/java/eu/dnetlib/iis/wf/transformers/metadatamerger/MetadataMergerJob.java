package eu.dnetlib.iis.wf.transformers.metadatamerger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.schemas.DocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.ExtractedDocumentMetadataMergedWithOriginal;
import eu.dnetlib.iis.transformers.metadatamerger.schemas.PublicationType;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;
import scala.Tuple2;

/**
 * Spark job that merges base document metadata (from importer) with metadata extracted from PDF documents.
 * Performs a full outer join on document id, selecting the first non-empty value for scalar fields,
 * merging arrays and maps, and assembling extracted author names from the extracted metadata.
 *
 * Replaces the PIG-based merger workflow step.
 */
public class MetadataMergerJob {

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        MetadataMergerJobParameters params = new MetadataMergerJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputMergedMetadata);

            JavaRDD<DocumentMetadata> baseMetadata = avroLoader.loadJavaRDD(sc, params.inputBaseMetadata,
                    DocumentMetadata.class);
            JavaRDD<ExtractedDocumentMetadata> extractedMetadata = avroLoader.loadJavaRDD(sc,
                    params.inputExtractedMetadata, ExtractedDocumentMetadata.class);

            JavaPairRDD<String, DocumentMetadata> baseById = baseMetadata
                    .mapToPair(doc -> new Tuple2<>(doc.getId().toString(), doc));
            JavaPairRDD<String, ExtractedDocumentMetadata> extractedById = extractedMetadata
                    .mapToPair(doc -> new Tuple2<>(doc.getId().toString(), doc));

            JavaRDD<ExtractedDocumentMetadataMergedWithOriginal> merged = baseById
                    .fullOuterJoin(extractedById)
                    .map(pair -> mergeRecords(pair._2._1.orElse(null), pair._2._2.orElse(null)));

            avroSaver.saveJavaRDD(merged, ExtractedDocumentMetadataMergedWithOriginal.SCHEMA$,
                    params.outputMergedMetadata);
        }
    }

    //------------------------ PRIVATE --------------------------

    private static ExtractedDocumentMetadataMergedWithOriginal mergeRecords(
            DocumentMetadata base, ExtractedDocumentMetadata extracted) {

        CharSequence id = firstNotEmptyStr(
                base != null ? base.getId() : null,
                extracted != null ? extracted.getId() : null);

        PublicationType publicationType;
        if (base != null && base.getPublicationType() != null) {
            eu.dnetlib.iis.importer.schemas.PublicationType srcType = base.getPublicationType();
            publicationType = PublicationType.newBuilder()
                    .setArticle(srcType.getArticle())
                    .setDataset(srcType.getDataset())
                    .build();
        } else {
            publicationType = PublicationType.newBuilder()
                    .setArticle(false)
                    .setDataset(false)
                    .build();
        }

        List<CharSequence> mergedKeywords = mergeStringLists(
                base != null ? base.getKeywords() : null,
                extracted != null ? extracted.getKeywords() : null);

        Map<CharSequence, CharSequence> mergedExternalIdentifiers = mergeStringMaps(
                base != null ? base.getExternalIdentifiers() : null,
                extracted != null ? extracted.getExternalIdentifiers() : null);

        List<CharSequence> extractedAuthorFullNames = extractAuthorFullNames(
                extracted != null ? extracted.getAuthors() : null);

        return ExtractedDocumentMetadataMergedWithOriginal.newBuilder()
                .setId(id)
                .setTitle(firstNotEmptyStr(
                        base != null ? base.getTitle() : null,
                        extracted != null ? extracted.getTitle() : null))
                .setAbstract$(firstNotEmptyStr(
                        base != null ? base.getAbstract$() : null,
                        extracted != null ? extracted.getAbstract$() : null))
                .setLanguage(firstNotEmptyStr(
                        base != null ? base.getLanguage() : null,
                        extracted != null ? extracted.getLanguage() : null))
                .setKeywords(mergedKeywords)
                .setExternalIdentifiers(mergedExternalIdentifiers)
                .setJournal(firstNotEmptyStr(
                        base != null ? base.getJournal() : null,
                        extracted != null ? extracted.getJournal() : null))
                .setYear(firstNotNullInt(
                        base != null ? base.getYear() : null,
                        extracted != null ? extracted.getYear() : null))
                .setPublisher(firstNotEmptyStr(
                        base != null ? base.getPublisher() : null,
                        extracted != null ? extracted.getPublisher() : null))
                .setPublicationType(publicationType)
                .setReferences(extracted != null ? extracted.getReferences() : null)
                .setExtractedAuthorFullNames(extractedAuthorFullNames)
                .setImportedAuthors(base != null ? base.getAuthors() : null)
                .setVolume(extracted != null ? extracted.getVolume() : null)
                .setIssue(extracted != null ? extracted.getIssue() : null)
                .setPages(extracted != null ? extracted.getPages() : null)
                .setPublicationTypeName(extracted != null ? extracted.getPublicationTypeName() : null)
                .build();
    }

    private static CharSequence firstNotEmptyStr(CharSequence first, CharSequence second) {
        if (first != null && first.length() > 0) {
            return first;
        }
        return second;
    }

    private static Integer firstNotNullInt(Integer first, Integer second) {
        return first != null ? first : second;
    }

    private static List<CharSequence> mergeStringLists(List<CharSequence> first, List<CharSequence> second) {
        List<CharSequence> merged = new ArrayList<>();
        if (first != null) {
            for (CharSequence item : first) {
                if (!merged.contains(item)) {
                    merged.add(item);
                }
            }
        }
        if (second != null) {
            for (CharSequence item : second) {
                if (!merged.contains(item)) {
                    merged.add(item);
                }
            }
        }
        return merged.isEmpty() ? null : merged;
    }

    private static Map<CharSequence, CharSequence> mergeStringMaps(
            Map<CharSequence, CharSequence> first, Map<CharSequence, CharSequence> second) {
        Map<CharSequence, CharSequence> merged = new HashMap<>();
        if (first != null) {
            merged.putAll(first);
        }
        if (second != null) {
            for (Map.Entry<CharSequence, CharSequence> entry : second.entrySet()) {
                if (!merged.containsKey(entry.getKey())) {
                    merged.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return merged.isEmpty() ? null : merged;
    }

    private static List<CharSequence> extractAuthorFullNames(
            List<eu.dnetlib.iis.metadataextraction.schemas.Author> authors) {
        if (authors == null) {
            return null;
        }
        List<CharSequence> names = new ArrayList<>();
        for (eu.dnetlib.iis.metadataextraction.schemas.Author author : authors) {
            if (author.getAuthorFullName() != null) {
                names.add(author.getAuthorFullName());
            }
        }
        return names.isEmpty() ? null : names;
    }

    @Parameters(separators = "=")
    private static class MetadataMergerJobParameters {

        @Parameter(names = "-inputBaseMetadata", required = true)
        private String inputBaseMetadata;

        @Parameter(names = "-inputExtractedMetadata", required = true)
        private String inputExtractedMetadata;

        @Parameter(names = "-outputMergedMetadata", required = true)
        private String outputMergedMetadata;
    }
}
