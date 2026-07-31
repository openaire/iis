package eu.dnetlib.iis.wf.export.actionmanager.entity.crossref;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.common.utils.RDDUtils;
import eu.dnetlib.iis.metadataextraction.schemas.ExtractedDocumentMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceBasicMetadata;
import eu.dnetlib.iis.metadataextraction.schemas.ReferenceMetadata;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionSerializationUtils;
import eu.dnetlib.iis.wf.export.actionmanager.IdentifierFactory;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Exporter job reading {@link ExtractedDocumentMetadata} Avro records and producing
 * {@link AtomicAction}<{@link Publication}> entities and {@link AtomicAction}<{@link Relation}>
 * links for each eligible reference.
 *
 * @author mhorst
 */
public class CrossrefExporterJob {

    private CrossrefExporterJob() {
    }

    private static final String INFERENCE_PROVENANCE = "iis::mutecitation_export";

    private static final String PID_TYPE = "mutecitation_";

    private static final String NUMERIC_PREFIX = InfoSpaceConstants.ROW_PREFIX_RESULT;

    private static final Qualifier RESULT_TYPE_PUBLICATION = buildResultTypePublication();

    private static final Qualifier INSTANCE_TYPE_PUBLICATION = buildInstanceTypePublication();

    private static final Qualifier MAIN_TITLE_QUALIFIER = buildMainTitleQualifier();

    private static final Qualifier RELEVANT_DATE_QUALIFIER = buildRelevantDateQualifier();

    private static final DataInfo OAF_ENTITY_DATAINFO = buildEntityDataInfo();

    private static final DataInfo OAF_RELATION_DATAINFO = buildRelationDataInfo();

    private static final String COUNTER_INPUT_RECORDS = "export.crossref.input.records";

    private static final String COUNTER_EXPORTED_ENTITIES = "export.crossref.entities";

    private static final String COUNTER_EXPORTED_UNIQUE_SOURCE_DOCS = "export.crossref.relations.uniqueSourceDocs";

    private static final int NUMBER_OF_OUTPUT_FILES = 10;

    private static SparkAvroLoader avroLoader = new SparkAvroLoader();

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    // ----------------------------------------- PUBLIC ----------------------------------------------

    public static void main(String[] args) throws Exception {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputEntityPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputRelationPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            JavaRDD<ExtractedDocumentMetadata> extractedDocMetaRDD = avroLoader
                    .loadJavaRDD(sc, params.inputPath, ExtractedDocumentMetadata.class);

            // Single pass: generate shared ids and produce both entity and relation actions
            JavaRDD<ExportEntry> exportEntries = extractedDocMetaRDD
                    .flatMap(docMeta -> {
                        List<ExportEntry> entries = new ArrayList<>();
                        if (docMeta.getReferences() != null) {
                            for (ReferenceMetadata ref : docMeta.getReferences()) {
                                if (isReferenceEligible(ref)) {
                                    String generatedId = IdentifierFactory.idFromPid(
                                            NUMERIC_PREFIX, PID_TYPE,
                                            ref.getBasicMetadata().getTitle().toString()
                                                    + UUID.randomUUID().toString(),
                                            true);
                                    entries.add(new ExportEntry(
                                            buildEntityAction(ref, generatedId),
                                            buildRelationAction(docMeta.getId().toString(), generatedId)));
                                }
                            }
                        }
                        return entries.iterator();
                    });

            JavaPairRDD<Text, Text> entitiesToExportRDD = AtomicActionSerializationUtils
                    .mapActionToText(exportEntries.map(e -> e.entityAction));
            JavaPairRDD<Text, Text> relationsToExportRDD = AtomicActionSerializationUtils
                    .mapActionToText(exportEntries.map(e -> e.relationAction));

            Configuration configuration = sc.hadoopConfiguration();
            configuration.set(FileOutputFormat.COMPRESS, Boolean.TRUE.toString());
            configuration.set(FileOutputFormat.COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.name());

            exportEntries.cache();

            RDDUtils.saveTextPairRDD(entitiesToExportRDD, NUMBER_OF_OUTPUT_FILES,
                    params.outputEntityPath, configuration);
            RDDUtils.saveTextPairRDD(relationsToExportRDD, NUMBER_OF_OUTPUT_FILES,
                    params.outputRelationPath, configuration);

            // generate report
            generateReport(sc, extractedDocMetaRDD, exportEntries, params.outputReportPath);
        }
    }

    // ----------------------------------------- PRIVATE ----------------------------------------------

    /**
     * Acceptance rule: title must not be blank AND at least one author must be present with non-blank name.
     */
    private static boolean isReferenceEligible(ReferenceMetadata ref) {
        if (ref.getBasicMetadata() == null) {
            return false;
        }
        ReferenceBasicMetadata basic = ref.getBasicMetadata();
        if (StringUtils.isBlank(basic.getTitle())) {
            return false;
        }
        if (basic.getAuthors() == null || basic.getAuthors().isEmpty()) {
            return false;
        }
        boolean hasNonBlankAuthor = false;
        for (CharSequence author : basic.getAuthors()) {
            if (StringUtils.isNotBlank(author)) {
                hasNonBlankAuthor = true;
                break;
            }
        }
        return hasNonBlankAuthor;
    }

    // ----------------------------------------- ENTITIES ----------------------------------------------

    private static AtomicAction<Publication> buildEntityAction(ReferenceMetadata ref, String generatedId) {
        AtomicAction<Publication> action = new AtomicAction<>();
        action.setClazz(Publication.class);
        action.setPayload(buildPublication(ref, generatedId));
        return action;
    }

    private static Publication buildPublication(ReferenceMetadata ref, String generatedId) {
        ReferenceBasicMetadata basic = ref.getBasicMetadata();

        Publication publication = new Publication();

        // id
        publication.setId(generatedId);

        // result type
        publication.setResulttype(RESULT_TYPE_PUBLICATION);

        // dataInfo
        publication.setDataInfo(OAF_ENTITY_DATAINFO);

        // title
        if (StringUtils.isNotBlank(basic.getTitle())) {
            StructuredProperty titleProp = new StructuredProperty();
            titleProp.setValue(basic.getTitle().toString());
            titleProp.setQualifier(MAIN_TITLE_QUALIFIER);
            publication.setTitle(Collections.singletonList(titleProp));
        }

        // authors
        if (basic.getAuthors() != null && !basic.getAuthors().isEmpty()) {
            List<Author> authorList = new ArrayList<>();
            int rank = 1;
            for (CharSequence author : basic.getAuthors()) {
                if (StringUtils.isNotBlank(author)) {
                    Author authorObj = new Author();
                    authorObj.setFullname(author.toString());
                    authorObj.setRank(rank);
                    authorList.add(authorObj);
                    rank++;
                }
            }
            if (!authorList.isEmpty()) {
                publication.setAuthor(authorList);
            }
        }

        // relevant date (year)
        if (StringUtils.isNotBlank(basic.getYear())) {
            String formattedDate = formatYear(basic.getYear().toString());
            if (formattedDate != null) {
                StructuredProperty dateProp = new StructuredProperty();
                dateProp.setValue(formattedDate);
                dateProp.setQualifier(RELEVANT_DATE_QUALIFIER);
                publication.setRelevantdate(Collections.singletonList(dateProp));

                // date of acceptance (year) propagated with the same formatted value
                Field<String> doaField = new Field<>();
                doaField.setValue(formattedDate);
                publication.setDateofacceptance(doaField);
            }
        }

        // instance type
        Instance instance = new Instance();
        instance.setInstancetype(INSTANCE_TYPE_PUBLICATION);
        publication.setInstance(Collections.singletonList(instance));

        publication.setLastupdatetimestamp(System.currentTimeMillis());

        return publication;
    }

    /**
     * Formats year to YYYY-MM-DD format.
     * If the input is in YYYY format, it becomes YYYY-01-01.
     * If already in YYYY-MM-DD format, it's copied as-is.
     * Otherwise returns null.
     */
    private static String formatYear(String year) {
        if (StringUtils.isBlank(year)) {
            return null;
        }
        String trimmed = year.trim();
        // YYYY-MM-DD format
        if (trimmed.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return trimmed;
        }
        // YYYY format
        if (trimmed.matches("\\d{4}")) {
            return trimmed + "-01-01";
        }
        return null;
    }

    // ----------------------------------------- RELATIONS ----------------------------------------------

    private static AtomicAction<Relation> buildRelationAction(String sourceId, String targetId) {
        AtomicAction<Relation> action = new AtomicAction<>();
        action.setClazz(Relation.class);
        action.setPayload(buildRelation(sourceId, targetId));
        return action;
    }

    private static Relation buildRelation(String source, String target) {
        Relation relation = new Relation();
        relation.setSource(source);
        relation.setTarget(target);
        relation.setRelType(OafConstants.REL_TYPE_RESULT_RESULT);
        relation.setSubRelType(OafConstants.SUBREL_TYPE_RELATIONSHIP);
        relation.setRelClass(OafConstants.REL_CLASS_CITES);
        relation.setDataInfo(OAF_RELATION_DATAINFO);
        relation.setLastupdatetimestamp(System.currentTimeMillis());
        return relation;
    }

    // ----------------------------------------- BUILDERS ----------------------------------------------

    private static Qualifier buildResultTypePublication() {
        return buildQualifier(
                "publication", "publication",
                InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES,
                InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES);
    }

    private static Qualifier buildInstanceTypePublication() {
        return buildQualifier(
                "0000", "UNKNOWN",
                "dnet:publication_resource", "dnet:publication_resource");
    }

    private static Qualifier buildMainTitleQualifier() {
        return buildQualifier(
                "main title", "main title",
                "dnet:dataCite_title", "dnet:dataCite_title");
    }

    private static Qualifier buildRelevantDateQualifier() {
        return buildQualifier(
                "created", "created",
                "dnet:dataCite_date", "dnet:dataCite_date");
    }

    private static Qualifier buildQualifier(String classId, String className, String schemeId, String schemeName) {
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid(classId);
        qualifier.setClassname(className);
        qualifier.setSchemeid(schemeId);
        qualifier.setSchemename(schemeName);
        return qualifier;
    }

    private static DataInfo buildEntityDataInfo() {
        DataInfo dataInfo = new DataInfo();
        dataInfo.setInvisible(true);
        dataInfo.setInferred(true);
        dataInfo.setTrust("0.7");
        dataInfo.setInferenceprovenance(INFERENCE_PROVENANCE);
        dataInfo.setProvenanceaction(buildProvenanceQualifier());
        return dataInfo;
    }

    private static DataInfo buildRelationDataInfo() {
        DataInfo dataInfo = new DataInfo();
        dataInfo.setInferred(true);
        dataInfo.setTrust("0.7");
        dataInfo.setInferenceprovenance(INFERENCE_PROVENANCE);
        dataInfo.setProvenanceaction(buildProvenanceQualifier());
        return dataInfo;
    }

    private static Qualifier buildProvenanceQualifier() {
        return buildQualifier(
                InfoSpaceConstants.SEMANTIC_CLASS_IIS,
                InfoSpaceConstants.SEMANTIC_CLASS_IIS,
                InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS,
                InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS);
    }

    // ----------------------------------------- INNER TYPES ----------------------------------------------

    /**
     * Holds a pair of entity and relation actions sharing the same generated id.
     */
    private static class ExportEntry {

        final AtomicAction<Publication> entityAction;
        final AtomicAction<Relation> relationAction;

        ExportEntry(AtomicAction<Publication> entityAction, AtomicAction<Relation> relationAction) {
            this.entityAction = entityAction;
            this.relationAction = relationAction;
        }
    }

    // ----------------------------------------- REPORT ----------------------------------------------

    private static void generateReport(JavaSparkContext sc,
            JavaRDD<ExtractedDocumentMetadata> inputRDD,
            JavaRDD<ExportEntry> exportEntriesRDD,
            String outputReportPath) {

        long inputRecordsCount = inputRDD.count();
        long exportedEntitiesCount = exportEntriesRDD.count();
        long uniqueSourceDocIdsCount = exportEntriesRDD
                .map(e -> e.relationAction.getPayload().getSource())
                .distinct()
                .count();

        JavaRDD<ReportEntry> report = sc.parallelize(Lists.newArrayList(
                ReportEntryFactory.createCounterReportEntry(COUNTER_INPUT_RECORDS, inputRecordsCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_EXPORTED_ENTITIES, exportedEntitiesCount),
                ReportEntryFactory.createCounterReportEntry(COUNTER_EXPORTED_UNIQUE_SOURCE_DOCS, uniqueSourceDocIdsCount)),
                1);

        avroSaver.saveJavaRDD(report, ReportEntry.SCHEMA$, outputReportPath);
    }

    // ----------------------------------------- PARAMETERS ----------------------------------------------

    @Parameters(separators = "=")
    private static class JobParameters {

        @Parameter(names = "-inputPath", required = true,
                description = "path to input ExtractedDocumentMetadata Avro datastore")
        private String inputPath;

        @Parameter(names = "-outputEntityPath", required = true,
                description = "path to output entity AtomicAction<Publication> sequence files")
        private String outputEntityPath;

        @Parameter(names = "-outputRelationPath", required = true,
                description = "path to output relation AtomicAction<Relation> sequence files")
        private String outputRelationPath;

        @Parameter(names = "-outputReportPath", required = true,
                description = "path to output report datastore")
        private String outputReportPath;
    }
}
