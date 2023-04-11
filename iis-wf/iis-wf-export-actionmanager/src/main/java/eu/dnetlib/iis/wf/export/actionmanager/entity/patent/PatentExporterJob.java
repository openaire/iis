package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.*;
import eu.dnetlib.dhp.schema.oaf.utils.IdentifierFactory;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.common.utils.DateTimeUtils;
import eu.dnetlib.iis.common.utils.IteratorUtils;
import eu.dnetlib.iis.common.utils.ListUtils;
import eu.dnetlib.iis.common.utils.RDDUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionSerializationUtils;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.entity.ConfidenceLevelUtils;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.BuilderModuleHelper;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Patent entity and relations exporter reading {@link DocumentToPatent} avro records and exporting them as entity and relation actions.
 *
 * @author pjacewicz
 */
public class PatentExporterJob {

    private static final String EPO = "EPO";
    private static final String EUROPEAN_PATENT_OFFICE__PATSTAT = "European Patent Office/PATSTAT";
    private static final String CLASS_EPO_ID = "epo_id";
    private static final String CLASS_EPO_NR_EPODOC = "epo_nr_epodoc";
    private static final String IPC = "IPC";
    private static final String INTERNATIONAL_PATENT_CLASSIFICATION = "International Patent Classification";
    private static final String PATENT_ENTITY_ID_PREFIX = "epopatstat__";
    private static final String IIS_ENTITIES_PATENT = "iis-entities-patent";
    private static final String INFERENCE_PROVENANCE = buildInferenceProvenance();
    private static final String PATENT_DATASOURCE_OPENAIRE_ID_PREFIX = InfoSpaceConstants.ROW_PREFIX_DATASOURCE + InfoSpaceConstants.OPENAIRE_ENTITY_ID_PREFIX + InfoSpaceConstants.ID_NAMESPACE_SEPARATOR;
    private static final String PATENT_RESULT_OPENAIRE_ID_PREFIX = InfoSpaceConstants.ROW_PREFIX_RESULT + PATENT_ENTITY_ID_PREFIX + InfoSpaceConstants.ID_NAMESPACE_SEPARATOR;
    private static final String PATENT_ID_PREFIX_EPO = buildRowPrefixDatasourceOpenaireEntityIdPrefixEpo();
    private static final KeyValue OAF_ENTITY_COLLECTEDFROM = buildOafEntityPatentKeyValue();
    private static final Qualifier OAF_ENTITY_RESULT_METADATA_RESULTTYPE = buildOafEntityResultMetadataResulttype();
    private static final Qualifier OAF_ENTITY_PID_QUALIFIER_CLASS_EPO_ID = buildOafEntityPidQualifierClassEpoId();
    private static final Qualifier OAF_ENTITY_PID_QUALIFIER_CLASS_EPO_NR_EPODOC = buildOafEntityPidQualifierClassEpoNrEpodoc();
    private static final Qualifier OAF_ENTITY_RESULT_METADATA_TITLE_QUALIFIER = buildOafEntityResultMetadataTitleQualifier();
    private static final Qualifier OAF_ENTITY_RESULT_METADATA_SUBJECT_QUALIFIER = buildOafEntityResultMetadataSubjectQualifier();
    private static final Qualifier OAF_ENTITY_RESULT_METADATA_RELEVANTDATE_QUALIFIER = buildOafEntityResultMetadataRelevantdateQualifier();
    private static final DataInfo OAF_ENTITY_DATAINFO = BuilderModuleHelper.buildInferenceForTrustLevel(false,
            StaticConfigurationProvider.ACTION_TRUST_0_9, INFERENCE_PROVENANCE, IIS_ENTITIES_PATENT);

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final int numberOfOutputFiles = 10;
    private static final PatentExportCounterReporter counterReporter = new PatentExportCounterReporter();

    private static final DateTimeFormatter PATENT_DATE_OF_COLLECTION_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    private static String buildInferenceProvenance() {
        return InfoSpaceConstants.SEMANTIC_CLASS_IIS + InfoSpaceConstants.INFERENCE_PROVENANCE_SEPARATOR + AlgorithmName.document_patent;
    }

    private static String buildRowPrefixDatasourceOpenaireEntityIdPrefixEpo() {
        return PATENT_DATASOURCE_OPENAIRE_ID_PREFIX + DigestUtils.md5Hex(EPO);
    }

    private static Qualifier buildOafEntityResultMetadataResulttype() {
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_PUBLICATION);
        qualifier.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_PUBLICATION);
        qualifier.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES);
        qualifier.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES);
        return qualifier;
    }

    private static Instance buildOafEntityResultInstance(Patent patent, String patentEpoUrlRoot, List<StructuredProperty> pid) {
        Instance instance = new Instance();
        instance.setInstancetype(buildOafEntityResultInstanceInstancetype());
        instance.setHostedby(buildOafEntityPatentKeyValue());
        instance.setCollectedfrom(buildOafEntityPatentKeyValue());
        instance.setUrl(Collections.singletonList(buildOafEntityResultInstanceUrl(patent, patentEpoUrlRoot)));
        instance.setAlternateIdentifier(pid);
        return instance;
    }

    private static String buildOafEntityResultInstanceUrl(Patent patent, String patentEpoUrlRoot) {
        return String.format("%s%s%s", patentEpoUrlRoot, patent.getApplnAuth(), patent.getApplnNr());
    }

    private static Qualifier buildOafEntityResultInstanceInstancetype() {
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_INSTANCE_TYPE_PATENT);
        qualifier.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_PATENT);
        qualifier.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PUBLICATION_RESOURCE);
        qualifier.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PUBLICATION_RESOURCE);
        return qualifier;
    }

    private static KeyValue buildOafEntityPatentKeyValue() {
        KeyValue keyValue = new KeyValue();
        keyValue.setKey(PATENT_ID_PREFIX_EPO);
        keyValue.setValue(EUROPEAN_PATENT_OFFICE__PATSTAT);
        return keyValue;
    }

    private static Qualifier buildOafEntityPidQualifierClassEpoId() {
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid(CLASS_EPO_ID);
        qualifier.setClassname(CLASS_EPO_ID);
        qualifier.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PID_TYPES);
        qualifier.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PID_TYPES);
        return qualifier;
    }

    private static Qualifier buildOafEntityPidQualifierClassEpoNrEpodoc() {
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid(CLASS_EPO_NR_EPODOC);
        qualifier.setClassname(CLASS_EPO_NR_EPODOC);
        qualifier.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PID_TYPES);
        qualifier.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PID_TYPES);
        return qualifier;
    }

    private static Qualifier buildOafEntityResultMetadataTitleQualifier() {
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE);
        qualifier.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_MAIN_TITLE);
        qualifier.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_TITLE);
        qualifier.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_TITLE);
        return qualifier;
    }

    private static Qualifier buildOafEntityResultMetadataSubjectQualifier() {
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid(IPC);
        qualifier.setClassname(INTERNATIONAL_PATENT_CLASSIFICATION);
        qualifier.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES);
        qualifier.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_CLASSIFICATION_TAXONOMIES);
        return qualifier;
    }

    private static Qualifier buildOafEntityResultMetadataRelevantdateQualifier() {
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_NAME_SUBMITTED);
        qualifier.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_NAME_SUBMITTED);
        qualifier.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_DATACITE_DATE);
        qualifier.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_DATACITE_DATE);
        return qualifier;
    }

    public static void main(String[] args) throws IOException {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputRelationPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputEntityPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            final Float confidenceLevelThreshold = ConfidenceLevelUtils
                    .evaluateConfidenceLevelThreshold(params.trustLevelThreshold);

            JavaRDD<DocumentToPatent> relMetaRDD = avroLoader
                    .loadJavaRDD(sc, params.inputDocumentToPatentPath, DocumentToPatent.class);
            JavaRDD<Patent> entMetaRDD = avroLoader
                    .loadJavaRDD(sc, params.inputPatentPath, Patent.class);

            JavaPairRDD<CharSequence, DocumentToPatent> validRelMetaByApplnNrRDD = relMetaRDD
                    .filter(x -> isRelationValidAndAboveThreshold(x, confidenceLevelThreshold))
                    .mapToPair(x -> new Tuple2<>(x.getApplnNr(), x));

            JavaPairRDD<CharSequence, Patent> validEntMetaByApplnNrRDD = entMetaRDD
                    .filter(PatentExporterJob::isMetadataValid)
                    .mapToPair(x -> new Tuple2<>(x.getApplnNr(), x));

            JavaRDD<PatentExportMetadata> patentExportMetaRDD = validRelMetaByApplnNrRDD
                    .join(validEntMetaByApplnNrRDD)
                    .values()
                    .map(x -> {
                        DocumentToPatent relMeta = x._1();
                        Patent entMeta = x._2();
                        String documentId = generateDocumentId(relMeta);
                        String patentId = generatePatentId(entMeta);
                        return new PatentExportMetadata(relMeta, entMeta, documentId, patentId);
                    });

            JavaRDD<PatentExportMetadata> patentExportMetaDedupRDD = deduplicateByHigherConfidenceLevel(
                    patentExportMetaRDD).cache();

            Configuration configuration = sc.hadoopConfiguration();
            configuration.set(FileOutputFormat.COMPRESS, Boolean.TRUE.toString());
            configuration.set(FileOutputFormat.COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.name());

            JavaPairRDD<Text, Text> relationsToExportRDD = relationsToExport(patentExportMetaDedupRDD);
            RDDUtils.saveTextPairRDD(relationsToExportRDD, numberOfOutputFiles, params.outputRelationPath, configuration);

            String patentDateOfCollection = DateTimeUtils.format(
                    LocalDateTime.parse(params.patentDateOfCollection, PATENT_DATE_OF_COLLECTION_FORMATTER));
            JavaPairRDD<Text, Text> entitiesToExportRDD = entitiesToExport(patentExportMetaDedupRDD,
                    patentDateOfCollection, params.patentEpoUrlRoot);
            RDDUtils.saveTextPairRDD(entitiesToExportRDD, numberOfOutputFiles, params.outputEntityPath, configuration);

            counterReporter.report(sc, patentExportMetaDedupRDD, params.outputReportPath);
        }
    }

    private static boolean isRelationValidAndAboveThreshold(DocumentToPatent relation, Float confidenceLevelThreshold) {
        return ConfidenceLevelUtils.isValidConfidenceLevel(relation.getConfidenceLevel(), confidenceLevelThreshold);
    }

    private static boolean isMetadataValid(Patent meta) {
        return StringUtils.isNotBlank(meta.getApplnTitle());
    }

    private static String generateDocumentId(DocumentToPatent relation) {
        return relation.getDocumentId().toString();
    }

    private static String generatePatentId(Patent meta) {
        return PATENT_RESULT_OPENAIRE_ID_PREFIX +
                DigestUtils.md5Hex(meta.getApplnAuth().toString() + meta.getApplnNr().toString());
    }

    private static JavaRDD<PatentExportMetadata> deduplicateByHigherConfidenceLevel(JavaRDD<PatentExportMetadata> rdd) {
        return rdd
                .groupBy(x -> new Tuple2<>(x.getDocumentId(), x.getPatentId()))
                .mapValues(xs -> IteratorUtils.toStream(xs.iterator()).reduce(PatentExporterJob::reduceByHigherConfidenceLevel))
                .filter(x -> x._2.isPresent())
                .mapValues(Optional::get)
                .values();
    }

    private static PatentExportMetadata reduceByHigherConfidenceLevel(PatentExportMetadata x, PatentExportMetadata y) {
        return x.getDocumentToPatent().getConfidenceLevel() > y.getDocumentToPatent().getConfidenceLevel() ? x : y;
    }

    private static JavaPairRDD<Text, Text> relationsToExport(JavaRDD<PatentExportMetadata> rdd) {
        return AtomicActionSerializationUtils
                .mapActionToText(rdd
                        .flatMap(x -> {
                            String documentId = x.getDocumentId();
                            String patentId = x.getPatentId();
                            Float confidenceLevel = x.getDocumentToPatent().getConfidenceLevel();
                            return buildRelationActions(documentId, patentId, confidenceLevel).iterator();
                        })
                );
    }

    private static List<AtomicAction<Relation>> buildRelationActions(String documentId, String patentId, Float confidenceLevel) {
        AtomicAction<Relation> forwardAction = new AtomicAction<>();
        forwardAction.setClazz(Relation.class);
        forwardAction.setPayload(buildRelation(documentId, patentId, confidenceLevel));

        AtomicAction<Relation> reverseAction = new AtomicAction<>();
        reverseAction.setClazz(Relation.class);
        reverseAction.setPayload(buildRelation(patentId, documentId, confidenceLevel));

        return Arrays.asList(forwardAction, reverseAction);
    }

    private static Relation buildRelation(String source, String target, Float confidenceLevel) {
        Relation relation = new Relation();
        relation.setRelType(OafConstants.REL_TYPE_RESULT_RESULT);
        relation.setSubRelType(OafConstants.SUBREL_TYPE_RELATIONSHIP);
        relation.setRelClass(OafConstants.REL_CLASS_ISRELATEDTO);

        relation.setSource(source);
        relation.setTarget(target);

        relation.setDataInfo(BuilderModuleHelper.buildInferenceForConfidenceLevel(confidenceLevel, INFERENCE_PROVENANCE));
        relation.setLastupdatetimestamp(System.currentTimeMillis());

        return relation;
    }

    private static JavaPairRDD<Text, Text> entitiesToExport(JavaRDD<PatentExportMetadata> rdd,
                                                            String patentDateOfCollection,
                                                            String patentEpoUrlRoot) {
        JavaRDD<AtomicAction<Publication>> atomicActions = rdd
                .mapToPair(x -> new Tuple2<>(x.getPatentId(), x.getPatent()))
                .distinct()
                .map(x -> {
                    String patentId = x._1();
                    Patent patent = x._2();
                    return buildEntityAction(patent, patentId, patentDateOfCollection, patentEpoUrlRoot);
                });

        return AtomicActionSerializationUtils.mapActionToText(atomicActions);
    }

    private static AtomicAction<Publication> buildEntityAction(Patent patent, String patentId,
                                                               String patentDateOfCollection, String patentEpoUrlRoot) {
        AtomicAction<Publication> action = new AtomicAction<>();
        action.setClazz(Publication.class);
        action.setPayload(buildOafInstance(patent, patentId, patentDateOfCollection, patentEpoUrlRoot));
        return action;
    }

    private static StructuredProperty buildOafEntityPid(String value, Qualifier qualifier) {
        StructuredProperty pid = new StructuredProperty();
        pid.setValue(value);
        pid.setQualifier(qualifier);
        return pid;
    }

    private static Publication buildOafInstance(Patent patent,
                                                String patentId,
                                                String patentDateOfCollection,
                                                String patentEpoUrlRoot) {
        Publication result = new Publication();

        result.setLastupdatetimestamp(System.currentTimeMillis());
        result.setCollectedfrom(Collections.singletonList(OAF_ENTITY_COLLECTEDFROM));

        result.setDateofcollection(patentDateOfCollection);
        result.setDateoftransformation(patentDateOfCollection);
        result.setResulttype(OAF_ENTITY_RESULT_METADATA_RESULTTYPE);
        result.setDataInfo(OAF_ENTITY_DATAINFO);

        if (StringUtils.isNotBlank(patent.getApplnTitle())) {
            result.setTitle(Collections.singletonList(buildOafEntityResultMetadataTitle(patent.getApplnTitle())));
        }

        if (StringUtils.isNotBlank(patent.getApplnAbstract())) {
            result.setDescription(Collections.singletonList(buildOafEntityResultMetadataDescription(patent.getApplnAbstract())));
        }

        if (StringUtils.isNotBlank(patent.getEarliestPublnDate())) {
            result.setDateofacceptance(buildOafEntityResultMetadataDateofacceptance(patent.getEarliestPublnDate()));
        }

        if (StringUtils.isNotBlank(patent.getApplnFilingDate())) {
            result.setRelevantdate((Collections.singletonList(buildOafEntityResultMetadataRelevantdate(patent.getApplnFilingDate()))));
        }

        if (Objects.nonNull(patent.getIpcClassSymbol())) {
            result.setSubject(buildOafEntityResultMetadataSubjects(patent.getIpcClassSymbol()));
        }

        if (Objects.nonNull(patent.getApplicantNames())) {
            result.setAuthor(buildOafEntityResultMetadataAuthors(patent.getApplicantNames()));
        }
        if (Objects.nonNull(patent.getApplicantCountryCodes())) {
            result.setCountry(buildOafEntityResultMetadataCountries(patent.getApplicantCountryCodes()));
        }

        List<StructuredProperty> pid = Lists.newArrayList();
        pid.add(buildOafEntityPid(String.format("%s%s", patent.getApplnAuth(), patent.getApplnNr()), OAF_ENTITY_PID_QUALIFIER_CLASS_EPO_ID));
        if (StringUtils.isNotBlank(patent.getApplnNrEpodoc())) {
            pid.add(buildOafEntityPid(patent.getApplnNrEpodoc().toString(), OAF_ENTITY_PID_QUALIFIER_CLASS_EPO_NR_EPODOC));
        }
        result.setInstance(Collections.singletonList(buildOafEntityResultInstance(patent, patentEpoUrlRoot, pid)));

        setId(result, patentId);

        return result;
    }

    private static StructuredProperty buildOafEntityResultMetadataTitle(CharSequence applnTitle) {
        StructuredProperty subject = new StructuredProperty();
        subject.setValue(applnTitle.toString());
        subject.setQualifier(OAF_ENTITY_RESULT_METADATA_TITLE_QUALIFIER);
        return subject;
    }

    private static Field<String> buildOafEntityResultMetadataDescription(CharSequence applnAbstract) {
        Field<String> result = new Field<>();
        result.setValue(applnAbstract.toString());
        return result;
    }

    private static List<Subject> buildOafEntityResultMetadataSubjects(List<CharSequence> ipcClassSymbols) {
        return ipcClassSymbols.stream()
                .filter(StringUtils::isNotBlank)
                .map(PatentExporterJob::buildOafEntityResultMetadataSubject)
                .collect(Collectors.toList());
    }

    private static Subject buildOafEntityResultMetadataSubject(CharSequence ipcClassSymbol) {
    	Subject subject = new Subject();
        subject.setValue(ipcClassSymbol.toString());
        subject.setQualifier(OAF_ENTITY_RESULT_METADATA_SUBJECT_QUALIFIER);
        return subject;
    }

    private static Field<String> buildOafEntityResultMetadataDateofacceptance(CharSequence earliestPublnDate) {
        Field<String> result = new Field<>();
        result.setValue(earliestPublnDate.toString());
        return result;
    }

    private static StructuredProperty buildOafEntityResultMetadataRelevantdate(CharSequence applnFilingDate) {
        StructuredProperty relevantDate = new StructuredProperty();
        relevantDate.setValue(applnFilingDate.toString());
        relevantDate.setQualifier(OAF_ENTITY_RESULT_METADATA_RELEVANTDATE_QUALIFIER);
        return relevantDate;
    }

    private static List<Author> buildOafEntityResultMetadataAuthors(List<CharSequence> applicantNames) {
        List<CharSequence> personNames = applicantNames.stream()
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        return ListUtils.zipWithIndex(personNames).stream()
                .map(pair -> {
                    Integer rank = pair.getLeft() + 1;
                    CharSequence personName = pair.getRight();
                    return buildOafEntityResultMetadataAuthor(personName, rank);
                })
                .collect(Collectors.toList());
    }

    private static Author buildOafEntityResultMetadataAuthor(CharSequence personName, Integer rank) {
        Author author = new Author();
        author.setFullname(personName.toString());
        author.setRank(rank);
        return author;
    }

    private static List<Country> buildOafEntityResultMetadataCountries(List<CharSequence> applicantCountryCodes) {
        return applicantCountryCodes.stream()
                .filter(StringUtils::isNotBlank)
                .distinct()
                .sorted()
                .map(PatentExporterJob::buildOafEntityResultMetadataCountry)
                .collect(Collectors.toList());
    }

    private static Country buildOafEntityResultMetadataCountry(CharSequence sourceCountry) {
        Country country = new Country();
        country.setClassid(sourceCountry.toString());
        country.setClassname(sourceCountry.toString());
        country.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_COUNTRIES);
        country.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_COUNTRIES);
        return country;
    }

    private static void setId(Publication publication, String fallbackId) {
        publication.setId(fallbackId);
        publication.setId(IdentifierFactory.createIdentifier(publication));
    }

    @Parameters(separators = "=")
    private static class JobParameters {
        @Parameter(names = "-inputDocumentToPatentPath", required = true)
        private String inputDocumentToPatentPath;

        @Parameter(names = "-inputPatentPath", required = true)
        private String inputPatentPath;

        @Parameter(names = "-outputEntityPath", required = true)
        private String outputEntityPath;

        @Parameter(names = "-outputRelationPath", required = true)
        private String outputRelationPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;

        @Parameter(names = "-trustLevelThreshold")
        private String trustLevelThreshold;

        @Parameter(names = "-patentDateOfCollection", required = true)
        private String patentDateOfCollection;

        @Parameter(names = "-patentEpoUrlRoot", required = true)
        private String patentEpoUrlRoot;
    }
}
