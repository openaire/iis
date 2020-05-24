package eu.dnetlib.iis.wf.importer.patent;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.referenceextraction.patent.schemas.HolderCountry;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Spark job responsible for reading {@link Patent} records from JSON file.
 *
 * @author mhorst
 */
public class PatentReaderJob {

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    public static final Logger log = Logger.getLogger(PatentReaderJob.class);

    private static final String COUNTER_READ_TOTAL = "import.patents";
    private static final String FIELD_APPLN_ID = "appln_id";
    private static final String FIELD_APPLN_AUTH = "appln_auth";
    private static final String FIELD_APPLN_NR = "appln_nr";
    private static final String FIELD_APPLN_FILING_DATE = "appln_filing_date";
    private static final String FIELD_APPLN_NR_EPODOC = "appln_nr_epodoc";
    private static final String FIELD_EARLIEST_PUBLN_DATE = "earliest_publn_date";
    private static final String FIELD_APPLN_ABSTRACT = "appln_abstract";
    private static final String FIELD_APPLN_TITLE = "appln_title";
    private static final String FIELD_IPC_CLASS_SYMBOL = "ipc_class_symbol";
    private static final String FIELD_HOLDER_COUNTRY = "holder_country";
    private static final String FIELD_HOLDER_COUNTRY__PERSON_NAME = "person_name";
    private static final String FIELD_HOLDER_COUNTRY__PERSON_CTRY_CODE = "person_ctry_code";

    private static final String ARRAY_STRING_START_TAG = "[";
    private static final String ARRAY_STRING_END_TAG = "]";
    private static final String ARRAY_STRING_DELIMITER = ",";
    private static final String NULL_STRING = "NULL";

    private static final StructType PATENTS_EPO_FILE_SCHEMA = buildPatentsEpoFileSchema();

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws Exception {

        PatentReaderJobParameters params = new PatentReaderJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputPath);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            SQLContext sqlContext = new SQLContext(sc);
            JavaRDD<Patent> results = sqlContext.read()
                    .schema(PATENTS_EPO_FILE_SCHEMA)
                    .json(params.inputJSONLocation)
                    .toJavaRDD()
                    .filter(PatentReaderJob::isValidPatentRow)
                    .map(PatentReaderJob::buildEntry);

            storeInOutput(results, generateReportEntries(sc, results), params.outputPath, params.outputReportPath);
        }
    }

    //------------------------ PRIVATE --------------------------

    private static StructType buildPatentsEpoFileSchema() {
        return StructType$.MODULE$.apply(ImmutableList.<StructField>builder()
                .add(buildApplnIdSchema())
                .add(buildApplnAuthSchema())
                .add(buildApplnNrSchema())
                .add(buildApplnFilingDateSchema())
                .add(buildApplnNrEpodocSchema())
                .add(buildEarliestPublnDateSchema())
                .add(buildApplnAbstractSchema())
                .add(buildApplnTitleSchema())
                .add(buildIpcClassSymbolSchema())
                .add(buildHolderCountrySchema())
                .build());
    }

    private static StructField buildApplnIdSchema() {
        return StructField$.MODULE$.apply(FIELD_APPLN_ID, DataTypes.StringType, false, Metadata.empty());
    }

    private static StructField buildApplnAuthSchema() {
        return StructField$.MODULE$.apply(FIELD_APPLN_AUTH, DataTypes.StringType, false, Metadata.empty());
    }

    private static StructField buildApplnNrSchema() {
        return StructField$.MODULE$.apply(FIELD_APPLN_NR, DataTypes.StringType, false, Metadata.empty());
    }

    private static StructField buildApplnFilingDateSchema() {
        return StructField$.MODULE$.apply(FIELD_APPLN_FILING_DATE, DataTypes.StringType, true, Metadata.empty());
    }

    private static StructField buildApplnNrEpodocSchema() {
        return StructField$.MODULE$.apply(FIELD_APPLN_NR_EPODOC, DataTypes.StringType, false, Metadata.empty());
    }

    private static StructField buildEarliestPublnDateSchema() {
        return StructField$.MODULE$.apply(FIELD_EARLIEST_PUBLN_DATE, DataTypes.StringType, true, Metadata.empty());
    }

    private static StructField buildApplnAbstractSchema() {
        return StructField$.MODULE$.apply(FIELD_APPLN_ABSTRACT, DataTypes.StringType, true, Metadata.empty());
    }

    private static StructField buildApplnTitleSchema() {
        return StructField$.MODULE$.apply(FIELD_APPLN_TITLE, DataTypes.StringType, true, Metadata.empty());
    }

    private static StructField buildIpcClassSymbolSchema() {
        return StructField$.MODULE$.apply(FIELD_IPC_CLASS_SYMBOL, DataTypes.StringType, true, Metadata.empty());
    }

    private static StructField buildHolderCountrySchema() {
        return StructField$.MODULE$.apply(FIELD_HOLDER_COUNTRY, DataTypes.createArrayType(DataTypes.createStructType(
                ImmutableList.of(buildHolderCountryPersonNameSchema(), buildHolderCountryPersonCtryCodeSchema()
                )), true), true, Metadata.empty());
    }

    private static StructField buildHolderCountryPersonNameSchema() {
        return StructField$.MODULE$.apply(FIELD_HOLDER_COUNTRY__PERSON_NAME, DataTypes.StringType, true, Metadata.empty());
    }

    private static StructField buildHolderCountryPersonCtryCodeSchema() {
        return StructField$.MODULE$.apply(FIELD_HOLDER_COUNTRY__PERSON_CTRY_CODE, DataTypes.StringType, true, Metadata.empty());
    }

    private static Boolean isValidPatentRow(Row row) {
        boolean isApplnIdValid = Objects.nonNull(fieldValueOrNull(row, FIELD_APPLN_ID, row::getAs));
        boolean isApplnAuthValid = Objects.nonNull(fieldValueOrNull(row, FIELD_APPLN_AUTH, row::getAs));
        boolean isApplnNrValid = Objects.nonNull(fieldValueOrNull(row, FIELD_APPLN_NR, row::getAs));
        boolean isApplnNrEpodocValid = Objects.nonNull(fieldValueOrNull(row, FIELD_APPLN_NR_EPODOC, row::getAs));
        return isApplnIdValid && isApplnAuthValid && isApplnNrValid && isApplnNrEpodocValid;
    }

    private static Patent buildEntry(Row row) {
        return Patent.newBuilder()
                .setApplnId(buildApplnId(row))
                .setApplnAuth(buildApplnAuth(row))
                .setApplnNr(buildApplnNr(row))
                .setApplnFilingDate(buildApplnFilingDate(row))
                .setApplnNrEpodoc(buildApplnNrEpodoc(row))
                .setEarliestPublnDate(buildEarliestPublnDate(row))
                .setApplnAbstract(buildApplnAbstract(row))
                .setApplnTitle(buildApplnTitle(row))
                .setIpcClassSymbol(buildIpcClassSymbolList(row))
                .setHolderCountry(buildHolderCountryList(row))
                .build();
    }

    private static CharSequence buildApplnId(Row row) {
        return row.getAs(FIELD_APPLN_ID);
    }

    private static CharSequence buildApplnAuth(Row row) {
        return row.getAs(FIELD_APPLN_AUTH);
    }

    private static CharSequence buildApplnNr(Row row) {
        return row.getAs(FIELD_APPLN_NR);
    }

    private static CharSequence buildApplnFilingDate(Row row) {
        return fieldValueOrNull(row, FIELD_APPLN_FILING_DATE, row::getAs);
    }

    private static CharSequence buildApplnNrEpodoc(Row row) {
        return row.getAs(FIELD_APPLN_NR_EPODOC);
    }

    private static CharSequence buildEarliestPublnDate(Row row) {
        return fieldValueOrNull(row, FIELD_EARLIEST_PUBLN_DATE, row::getAs);
    }

    private static CharSequence buildApplnAbstract(Row row) {
        return fieldValueOrNull(row, FIELD_APPLN_ABSTRACT, row::getAs);
    }

    private static CharSequence buildApplnTitle(Row row) {
        return fieldValueOrNull(row, FIELD_APPLN_TITLE, row::getAs);
    }

    private static List<CharSequence> buildIpcClassSymbolList(Row row) {
        return Optional
                .ofNullable((String) fieldValueOrNull(row, FIELD_IPC_CLASS_SYMBOL, row::getAs))
                .map(raw -> Arrays.stream(StringUtils
                        .strip(raw.replace("\"", ""), ARRAY_STRING_START_TAG + ARRAY_STRING_END_TAG)
                        .split(ARRAY_STRING_DELIMITER))
                        .filter(StringUtils::isNotBlank)
                        .filter(x -> !NULL_STRING.equalsIgnoreCase(x))
                        .map(x -> (CharSequence) x)
                        .collect(Collectors.toList())
                )
                .orElse(null);
    }

    private static List<HolderCountry> buildHolderCountryList(Row row) {
        return Optional
                .ofNullable(fieldValueOrNull(row, FIELD_HOLDER_COUNTRY, s -> row.<Row>getList(row.fieldIndex(FIELD_HOLDER_COUNTRY))))
                .map(list -> list.stream().map(PatentReaderJob::buildHolderCountry).collect(Collectors.toList()))
                .orElse(null);
    }

    private static HolderCountry buildHolderCountry(Row row) {
        return HolderCountry.newBuilder()
                .setPersonName(fieldValueOrNull(row, FIELD_HOLDER_COUNTRY__PERSON_NAME, row::getAs))
                .setPersonCtryCode(fieldValueOrNull(row, FIELD_HOLDER_COUNTRY__PERSON_CTRY_CODE, row::getAs))
                .build();
    }

    private static <X> X fieldValueOrNull(Row row, String fieldName, Function<String, X> getter) {
        if (!row.isNullAt(row.fieldIndex(fieldName))) {
            return getter.apply(fieldName);
        }
        return null;
    }

    private static JavaRDD<ReportEntry> generateReportEntries(JavaSparkContext sparkContext,
                                                              JavaRDD<Patent> entries) {
        Preconditions.checkNotNull(sparkContext, "sparkContext has not been set");
        ReportEntry fromCacheEntitiesCounter = ReportEntryFactory.createCounterReportEntry(COUNTER_READ_TOTAL, entries.count());
        return sparkContext.parallelize(Lists.newArrayList(fromCacheEntitiesCounter));
    }

    private static void storeInOutput(JavaRDD<Patent> results, JavaRDD<ReportEntry> reports,
                                      String resultOutputPath, String reportOutputPath) {
        avroSaver.saveJavaRDD(results, Patent.SCHEMA$, resultOutputPath);
        avroSaver.saveJavaRDD(reports, ReportEntry.SCHEMA$, reportOutputPath);
    }

    @Parameters(separators = "=")
    private static class PatentReaderJobParameters {
        @Parameter(names = "-inputJSONLocation", required = true)
        private String inputJSONLocation;

        @Parameter(names = "-outputPath", required = true)
        private String outputPath;

        @Parameter(names = "-outputReportPath", required = true)
        private String outputReportPath;
    }
}
