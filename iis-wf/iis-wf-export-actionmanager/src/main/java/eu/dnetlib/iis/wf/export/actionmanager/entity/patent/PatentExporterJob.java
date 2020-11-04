package eu.dnetlib.iis.wf.export.actionmanager.entity.patent;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import eu.dnetlib.iis.common.utils.IteratorUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Country;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.utils.ListUtils;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.common.utils.DateTimeUtils;
import eu.dnetlib.iis.common.utils.RDDUtils;
import eu.dnetlib.iis.referenceextraction.patent.schemas.DocumentToPatent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.wf.export.actionmanager.AtomicActionSerializationUtils;
import eu.dnetlib.iis.wf.export.actionmanager.OafConstants;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.entity.ConfidenceLevelUtils;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.BuilderModuleHelper;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import scala.Tuple2;

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

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        JobParameters params = new JobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        Configuration configuration = Job.getInstance().getConfiguration();
        configuration.set(FileOutputFormat.COMPRESS, Boolean.TRUE.toString());
        configuration.set(FileOutputFormat.COMPRESS_TYPE, SequenceFile.CompressionType.BLOCK.name());

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputRelationPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputEntityPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            Float trustLevelThreshold = ConfidenceLevelUtils.evaluateConfidenceLevelThreshold(params.trustLevelThreshold);

            JavaRDD<DocumentToPatent> documentToPatents = avroLoader
                    .loadJavaRDD(sc, params.inputDocumentToPatentPath, DocumentToPatent.class);
            JavaRDD<Patent> patents = avroLoader
                    .loadJavaRDD(sc, params.inputPatentPath, Patent.class);

            JavaRDD<DocumentToPatent> documentToPatentsToExport =
                    documentToPatentsToExport(documentToPatents, trustLevelThreshold);

            JavaPairRDD<CharSequence, Patent> validPatentsById = patents
                    .filter(PatentExporterJob::isValidPatent)
                    .mapToPair(x -> new Tuple2<>(x.getApplnNr(), x))
                    .cache();

            JavaRDD<DocumentToPatentWithIdsToExport> documentToPatentsToExportWithIds = documentToPatentsToExport
                    .mapToPair(x -> new Tuple2<>(x.getApplnNr(), x))
                    .join(validPatentsById)
                    .map(x -> {
                        DocumentToPatent documentToPatent = x._2()._1();
                        Patent patent = x._2()._2();
                        return new DocumentToPatentWithIdsToExport(x._2()._1(), documentIdToExport(documentToPatent.getDocumentId()),
                                patentIdToExport(patent.getApplnAuth(), patent.getApplnNr()));
                    })
                    .cache();
            
            JavaPairRDD<Text, Text> relationsToExport = relationsToExport(documentToPatentsToExportWithIds);

            RDDUtils.saveTextPairRDD(relationsToExport, numberOfOutputFiles, params.outputRelationPath, configuration);

            String patentDateOfCollection = DateTimeUtils.format(
                    LocalDateTime.parse(params.patentDateOfCollection, PATENT_DATE_OF_COLLECTION_FORMATTER));
            JavaPairRDD<Text, Text> entitiesToExport =
                    entitiesToExport(documentToPatentsToExportWithIds, validPatentsById, patentDateOfCollection,
                            params.patentEpoUrlRoot);
            
            RDDUtils.saveTextPairRDD(entitiesToExport, numberOfOutputFiles, params.outputEntityPath, configuration);

            counterReporter.report(sc, documentToPatentsToExportWithIds, params.outputReportPath);
        }
    }

    //------------------------ PRIVATE --------------------------

    private static String buildInferenceProvenance() {
        return InfoSpaceConstants.SEMANTIC_CLASS_IIS + InfoSpaceConstants.INFERENCE_PROVENANCE_SEPARATOR + AlgorithmName.document_patent;
    }

    private static String buildRowPrefixDatasourceOpenaireEntityIdPrefixEpo() {
        return appendMd5(PATENT_DATASOURCE_OPENAIRE_ID_PREFIX, EPO);
    }

    private static Qualifier buildOafEntityResultMetadataResulttype() {
        Qualifier qualifier = new Qualifier();
        qualifier.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_PUBLICATION);
        qualifier.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_PUBLICATION);
        qualifier.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES);
        qualifier.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_RESULT_TYPOLOGIES);
        return qualifier;
    }

    private static Instance buildOafEntityResultInstance(Patent patent, String patentEpoUrlRoot) {
        Instance instance = new Instance();
        instance.setInstancetype(buildOafEntityResultInstanceInstancetype());
        instance.setHostedby(buildOafEntityPatentKeyValue());
        instance.setCollectedfrom(buildOafEntityPatentKeyValue());
        instance.setUrl(Collections.singletonList(buildOafEntityResultInstanceUrl(patent, patentEpoUrlRoot)));
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

    private static JavaRDD<DocumentToPatent> documentToPatentsToExport(JavaRDD<DocumentToPatent> documentToPatents,
                                                                       Float trustLevelThreshold) {
        return documentToPatents
                .filter(x -> isValidDocumentToPatent(x, trustLevelThreshold))
                .groupBy(x -> new Tuple2<>(x.getDocumentId(), x.getApplnNr()))
                .mapValues(xs -> IteratorUtils.toStream(xs.iterator()).reduce(PatentExporterJob::reduceByConfidenceLevel))
                .filter(x -> x._2.isPresent())
                .mapValues(java.util.Optional::get)
                .values();
    }

    private static Boolean isValidDocumentToPatent(DocumentToPatent documentToPatent, Float trustLevelThreshold) {
        return ConfidenceLevelUtils.isValidConfidenceLevel(documentToPatent.getConfidenceLevel(), trustLevelThreshold);
    }
    
    private static boolean isValidPatent(Patent patent) {
        return StringUtils.isNotBlank(patent.getApplnTitle());
    }

    private static DocumentToPatent reduceByConfidenceLevel(DocumentToPatent x, DocumentToPatent y) {
        if (x.getConfidenceLevel() > y.getConfidenceLevel()) {
            return x;
        }
        return y;
    }

    private static  JavaPairRDD<Text, Text> relationsToExport(
            JavaRDD<DocumentToPatentWithIdsToExport> documentToPatentsToExportWithIds) {
        
        JavaRDD<AtomicAction<Relation>> result = documentToPatentsToExportWithIds.flatMap(x -> {
            DocumentToPatent documentToPatent = x.getDocumentToPatent();
            String documentIdToExport = x.getDocumentIdToExport();
            String patentIdToExport = x.getPatentIdToExport();
            return buildRelationActions(documentToPatent, documentIdToExport, patentIdToExport).iterator();
        });
        
        return AtomicActionSerializationUtils.mapActionToText(result);
        
    }
    
    

    private static List<AtomicAction<Relation>> buildRelationActions(DocumentToPatent documentToPatent,
                                                           String documentIdToExport,
                                                           String patentIdToExport) {

        AtomicAction<Relation> forwardAction = new AtomicAction<>();
        forwardAction.setClazz(Relation.class);
        forwardAction.setPayload(buildRelation(documentToPatent, documentIdToExport, patentIdToExport));
        
        AtomicAction<Relation> reverseAction = new AtomicAction<>();
        reverseAction.setClazz(Relation.class);
        reverseAction.setPayload(buildRelation(documentToPatent, patentIdToExport, documentIdToExport));

        return Arrays.asList(forwardAction, reverseAction);
    }

    private static Relation buildRelation(DocumentToPatent documentToPatent, String source, String target) {
        Relation relation = new Relation();
        relation.setRelType(OafConstants.REL_TYPE_RESULT_RESULT);
        relation.setSubRelType(OafConstants.SUBREL_TYPE_RELATIONSHIP);
        relation.setRelClass(OafConstants.REL_CLASS_ISRELATEDTO);
        relation.setSource(source);
        relation.setTarget(target);
        relation.setDataInfo(BuilderModuleHelper.buildInferenceForConfidenceLevel(documentToPatent.getConfidenceLevel(),
                INFERENCE_PROVENANCE));
        relation.setLastupdatetimestamp(System.currentTimeMillis());
        return relation;
    }

    private static JavaPairRDD<Text, Text> entitiesToExport(JavaRDD<DocumentToPatentWithIdsToExport> documentToPatentsToExportWithIds,
                                                            JavaPairRDD<CharSequence, Patent> patentsById,
                                                            String patentDateOfCollection,
                                                            String patentEpoUrlRoot) {
        JavaRDD<AtomicAction<Publication>> result = documentToPatentsToExportWithIds
                .mapToPair(x -> new Tuple2<>(x.getDocumentToPatent().getApplnNr(), x.getPatentIdToExport()))
                .distinct()
                .join(patentsById)
                .values()
                .map(x -> {
                    String patentIdToExport = x._1();
                    Patent patent = x._2();
                    return buildEntityAction(patent, patentIdToExport, patentDateOfCollection, patentEpoUrlRoot);
                });

        return AtomicActionSerializationUtils.mapActionToText(result);
    }

    private static AtomicAction<Publication> buildEntityAction(Patent patent, String patentIdToExport,
            String patentDateOfCollection, String patentEpoUrlRoot) {
        AtomicAction<Publication> action = new AtomicAction<>();
        action.setClazz(Publication.class);
        action.setPayload(buildOafPublication(patent, patentIdToExport, patentDateOfCollection, patentEpoUrlRoot));
        return action;
    }

    private static StructuredProperty buildOafEntityPid(String value, Qualifier qualifier) {
        StructuredProperty pid = new StructuredProperty();
        pid.setValue(value);
        pid.setQualifier(qualifier);
        return pid;
    }

    private static Publication buildOafPublication(Patent patent, String patentIdToExport, String patentDateOfCollection, String patentEpoUrlRoot) {
        Publication result = new Publication();
        result.setId(patentIdToExport);
        result.setLastupdatetimestamp(System.currentTimeMillis());
        result.setCollectedfrom(Collections.singletonList(OAF_ENTITY_COLLECTEDFROM));
        List<StructuredProperty> pids = Lists.newArrayList();
        pids.add(buildOafEntityPid(String.format("%s%s", patent.getApplnAuth(), patent.getApplnNr()), OAF_ENTITY_PID_QUALIFIER_CLASS_EPO_ID));
        if (StringUtils.isNotBlank(patent.getApplnNrEpodoc())) {
            pids.add(buildOafEntityPid(patent.getApplnNrEpodoc().toString(), OAF_ENTITY_PID_QUALIFIER_CLASS_EPO_NR_EPODOC));
        }
        result.setPid(pids);
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
        
        result.setInstance(Collections.singletonList(buildOafEntityResultInstance(patent, patentEpoUrlRoot)));
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

    private static String documentIdToExport(CharSequence documentId) {
        return documentId.toString();
    }

    private static String patentIdToExport(CharSequence applnAuth, CharSequence applnNr) {
        return appendMd5(PATENT_RESULT_OPENAIRE_ID_PREFIX, applnAuth.toString() + applnNr.toString());
    }

    private static List<StructuredProperty> buildOafEntityResultMetadataSubjects(List<CharSequence> ipcClassSymbols) {
        return ipcClassSymbols.stream()
                .filter(StringUtils::isNotBlank)
                .map(PatentExporterJob::buildOafEntityResultMetadataSubject)
                .collect(Collectors.toList());
    }

    private static StructuredProperty buildOafEntityResultMetadataSubject(CharSequence ipcClassSymbol) {
        StructuredProperty subject = new StructuredProperty();
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

    private static String appendMd5(String prefix, String suffix) {
        return prefix + DigestUtils.md5Hex(suffix);
    }

    @Parameters(separators = "=")
    private static class JobParameters {
        @Parameter(names = "-inputDocumentToPatentPath", required = true)
        private String inputDocumentToPatentPath;

        @Parameter(names = "-inputPatentPath", required = true)
        private String inputPatentPath;

        @Parameter(names = "-trustLevelThreshold")
        private String trustLevelThreshold;

        @Parameter(names = "-patentDateOfCollection", required = true)
        private String patentDateOfCollection;

        @Parameter(names = "-patentEpoUrlRoot", required = true)
        private String patentEpoUrlRoot;

        @Parameter(names = "-outputRelationPath", required = true)
        private String outputRelationPath;

        @Parameter(names = "-outputEntityPath", required = true)
        private String outputEntityPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
