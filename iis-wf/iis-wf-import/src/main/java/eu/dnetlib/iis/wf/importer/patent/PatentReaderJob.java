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
import eu.dnetlib.iis.referenceextraction.patent.schemas.HolderCountry;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Patent;
import eu.dnetlib.iis.referenceextraction.patent.schemas.Tls211PublnDateId;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
import java.util.stream.Collectors;

/**
 * Spark job responsible for reading {@link Patent} records from JSON file.
 *
 * @author mhorst
 */
public class PatentReaderJob {

    private static SparkAvroSaver avroSaver = new SparkAvroSaver();

    public static final Logger log = Logger.getLogger(PatentReaderJob.class);

    private static final String COUNTER_READ_TOTAL = "import.patent.read.total";
    private static final String FIELD_APPLN_ID = "appln_id";
    private static final String FIELD_APPLN_AUTH = "appln_auth";
    private static final String FIELD_APPLN_NR = "appln_nr";
    private static final String FIELD_APPLN_FILING_DATE = "appln_filing_date";
    private static final String FIELD_APPLN_NR_EPODOC = "appln_nr_epodoc";
    private static final String FIELD_EARLIEST_PUBLN_DATE = "earliest_publn_date";
    private static final String FIELD_APPLN_ABSTRACT = "appln_abstract";
    private static final String FIELD_APPLN_TITLE = "appln_title";
    private static final String FIELD_TLS211_PUBLN_DATE_ID = "tls211_publn_date_id";
    private static final String FIELD_TLS211_PUBLN_DATE_ID__PUBLN_DATE = "publn_date";
    private static final String FIELD_TLS211_PUBLN_DATE_ID__PAT_PUBLN_ID = "pat_publn_id";
    private static final String FIELD_IPC_CLASS_SYMBOL = "ipc_class_symbol";
    private static final String FIELD_HOLDER_COUNTRY = "holder_country";
    private static final String FIELD_HOLDER_COUNTRY__PERSON_NAME = "person_name";
    private static final String FIELD_HOLDER_COUNTRY__PERSON_CTRY_CODE = "person_ctry_code";

    private static final String ARRAY_STRING_START_TAG = "[";
    private static final String ARRAY_STRING_END_TAG = "]";
    private static final String ARRAY_STRING_DELIMITER = ",";

    private static final StructType PATENTS_EPO_FILE_SCHEMA = StructType$.MODULE$.apply(ImmutableList.<StructField>builder()
            .add(StructField$.MODULE$.apply(FIELD_APPLN_ID, DataTypes.StringType, false, Metadata.empty()))
            .add(StructField$.MODULE$.apply(FIELD_APPLN_AUTH, DataTypes.StringType, false, Metadata.empty()))
            .add(StructField$.MODULE$.apply(FIELD_APPLN_NR, DataTypes.StringType, false, Metadata.empty()))
            .add(StructField$.MODULE$.apply(FIELD_APPLN_FILING_DATE, DataTypes.StringType, false, Metadata.empty()))
            .add(StructField$.MODULE$.apply(FIELD_APPLN_NR_EPODOC, DataTypes.StringType, false, Metadata.empty()))
            .add(StructField$.MODULE$.apply(FIELD_EARLIEST_PUBLN_DATE, DataTypes.StringType, false, Metadata.empty()))
            .add(StructField$.MODULE$.apply(FIELD_APPLN_ABSTRACT, DataTypes.StringType, true, Metadata.empty()))
            .add(StructField$.MODULE$.apply(FIELD_APPLN_TITLE, DataTypes.StringType, false, Metadata.empty()))
            .add(StructField$.MODULE$.apply(FIELD_TLS211_PUBLN_DATE_ID, DataTypes.createArrayType(DataTypes.createStructType(
                    ImmutableList.of(
                            StructField$.MODULE$.apply(FIELD_TLS211_PUBLN_DATE_ID__PUBLN_DATE, DataTypes.StringType, false, Metadata.empty()),
                            StructField$.MODULE$.apply(FIELD_TLS211_PUBLN_DATE_ID__PAT_PUBLN_ID, DataTypes.StringType, false, Metadata.empty())
                    ))), false, Metadata.empty()))
            .add(StructField$.MODULE$.apply(FIELD_IPC_CLASS_SYMBOL, DataTypes.StringType, false, Metadata.empty()))
            .add(StructField$.MODULE$.apply(FIELD_HOLDER_COUNTRY, DataTypes.createArrayType(DataTypes.createStructType(
                    ImmutableList.of(
                            StructField$.MODULE$.apply(FIELD_HOLDER_COUNTRY__PERSON_NAME, DataTypes.StringType, false, Metadata.empty()),
                            StructField$.MODULE$.apply(FIELD_HOLDER_COUNTRY__PERSON_CTRY_CODE, DataTypes.StringType, false, Metadata.empty())
                    ))), false, Metadata.empty()))
            .build());

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws Exception {

        PatentReaderJobParameters params = new PatentReaderJobParameters();
        JCommander jcommander = new JCommander(params);
        jcommander.parse(args);

        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "pl.edu.icm.sparkutils.avro.AvroCompatibleKryoRegistrator");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            Configuration hadoopConf = sc.hadoopConfiguration();

            HdfsUtils.remove(hadoopConf, params.outputPath);
            HdfsUtils.remove(hadoopConf, params.outputReportPath);

            SQLContext sqlContext = new SQLContext(sc);

            JavaRDD<Patent> results = sqlContext.read()
                    .schema(PATENTS_EPO_FILE_SCHEMA)
                    .json(params.inputJSONLocation)
                    .toJavaRDD()
                    .map(PatentReaderJob::buildEntry);

            storeInOutput(results, generateReportEntries(sc, results), params.outputPath, params.outputReportPath);
        }
    }

    //------------------------ PRIVATE --------------------------

    private static Patent buildEntry(Row row) {
        Patent.Builder patentBuilder = Patent.newBuilder();
        patentBuilder.setApplnId(buildApplnId(row));
        patentBuilder.setApplnAuth(buildApplnAuth(row));
        patentBuilder.setApplnNr(buildApplnNr(row));
        patentBuilder.setApplnFilingDate(buildApplnFilingDate(row));
        patentBuilder.setApplnNrEpodoc(buildApplnNrEpodoc(row));
        patentBuilder.setEarliestPublnDate(buildEarliestPublnDate(row));
        patentBuilder.setApplnAbstract(buildApplnAbstract(row));
        patentBuilder.setApplnTitle(buildApplnTitle(row));
        patentBuilder.setTls211PublnDateId(buildTls211PublnDateId(row));
        patentBuilder.setIpcClassSymbol(buildIpcClassSymbol(row));
        patentBuilder.setHolderCountry(buildHolderCountry(row));
        return patentBuilder.build();
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
        return row.getAs(FIELD_APPLN_FILING_DATE);
    }

    private static CharSequence buildApplnNrEpodoc(Row row) {
        return row.getAs(FIELD_APPLN_NR_EPODOC);
    }

    private static CharSequence buildEarliestPublnDate(Row row) {
        return row.getAs(FIELD_EARLIEST_PUBLN_DATE);
    }

    private static CharSequence buildApplnAbstract(Row row) {
        if (Arrays.asList(row.schema().fieldNames()).contains(FIELD_APPLN_ABSTRACT)) {
            return row.getAs(FIELD_APPLN_ABSTRACT);
        }
        return null;
    }

    private static CharSequence buildApplnTitle(Row row) {
        return row.getAs(FIELD_APPLN_TITLE);
    }

    private static List<Tls211PublnDateId> buildTls211PublnDateId(Row row) {
        return row.getList(row.fieldIndex(FIELD_TLS211_PUBLN_DATE_ID)).stream()
                .map(elem -> {
                    Row elemRow = (Row) elem;
                    return Tls211PublnDateId.newBuilder()
                            .setPublnDate(elemRow.getAs(FIELD_TLS211_PUBLN_DATE_ID__PUBLN_DATE))
                            .setPatPublnId(elemRow.getAs(FIELD_TLS211_PUBLN_DATE_ID__PAT_PUBLN_ID))
                            .build();
                })
                .collect(Collectors.toList());
    }

    private static List<CharSequence> buildIpcClassSymbol(Row row) {
        String raw = row.getAs(FIELD_IPC_CLASS_SYMBOL);
        return Arrays.asList(StringUtils
                .strip(raw.replace("\"", ""), ARRAY_STRING_START_TAG + ARRAY_STRING_END_TAG)
                .split(ARRAY_STRING_DELIMITER));
    }

    private static List<HolderCountry> buildHolderCountry(Row row) {
        return row.getList(row.fieldIndex(FIELD_HOLDER_COUNTRY)).stream()
                .map(elem -> {
                    Row elemRow = (Row) elem;
                    return HolderCountry.newBuilder()
                            .setPersonName(elemRow.getAs(FIELD_HOLDER_COUNTRY__PERSON_NAME))
                            .setPersonCtryCode(elemRow.getAs(FIELD_HOLDER_COUNTRY__PERSON_CTRY_CODE))
                            .build();
                }).collect(Collectors.toList());
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
