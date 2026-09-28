package eu.dnetlib.iis.wf.importer.content;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.report.ReportEntryFactory;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.common.spark.JavaSparkContextFactory;
import eu.dnetlib.iis.importer.auxiliary.schemas.DocumentContentUrl;
import pl.edu.icm.sparkutils.avro.SparkAvroLoader;
import pl.edu.icm.sparkutils.avro.SparkAvroSaver;

/**
 * Spark port of {@link DocumentContentUrlDispatcher}.
 *
 * <p>Dispatches {@link DocumentContentUrl} records to dedicated output ports based on their
 * (lower-cased) mime type. The port definitions (name, mime types, report entry key) mirror the
 * Oozie {@code eu/dnetlib/iis/wf/importer/content_url/chain/oozie_app} workflow configuration.</p>
 *
 * <p>Records with a null mime type or with a mime type not assigned to any port are dropped
 * (with a warning), exactly like the MapReduce implementation.</p>
 */
public class DocumentContentUrlDispatcherJob {

    private static final Logger log = Logger.getLogger(DocumentContentUrlDispatcherJob.class);

    private static final SparkAvroLoader avroLoader = new SparkAvroLoader();
    private static final SparkAvroSaver avroSaver = new SparkAvroSaver();

    private static final String UNDEFINED = "$UNDEFINED$";

    private DocumentContentUrlDispatcherJob() {
    }

    //------------------------ LOGIC --------------------------

    public static void main(String[] args) throws IOException {
        Params params = new Params();
        new JCommander(params).parse(args);

        try (JavaSparkContext sc = JavaSparkContextFactory.withConfAndKryo(new SparkConf())) {
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputRoot);
            HdfsUtils.remove(sc.hadoopConfiguration(), params.outputReportPath);

            JavaRDD<DocumentContentUrl> input = avroLoader.loadJavaRDD(sc, params.input,
                    DocumentContentUrl.class);
            input.cache();

            List<Port> ports = params.ports();
            Map<String, LongAccumulator> counters = new HashMap<>();
            for (Port port : ports) {
                counters.put(port.name, sc.sc().longAccumulator("DISPATCHED_" + port.name));
            }
            final Map<String, String> mimeTypeToPort = buildMimeTypeToPortMap(ports);

            // single pass assigning each record to a port (null = unhandled, dropped)
            input.foreach(record -> {
                String portName = resolvePortName(record, mimeTypeToPort);
                if (portName != null) {
                    counters.get(portName).add(1);
                }
            });

            List<ReportEntry> reports = new ArrayList<>();
            for (Port port : ports) {
                JavaRDD<DocumentContentUrl> portRdd = input.filter(
                        record -> port.name.equals(resolvePortName(record, mimeTypeToPort)));
                if (port.reportEntryKey != null) {
                    reports.add(ReportEntryFactory.createCounterReportEntry(port.reportEntryKey,
                            counters.get(port.name).value()));
                }
                avroSaver.saveJavaRDD(portRdd, DocumentContentUrl.SCHEMA$,
                        params.outputRoot + "/" + port.name);
            }

            if (params.outputReportPath != null) {
                avroSaver.saveJavaRDD(sc.parallelize(reports).repartition(1), ReportEntry.SCHEMA$,
                        params.outputReportPath);
            }
        }
    }

    //------------------------ PRIVATE --------------------------

    private static Map<String, String> buildMimeTypeToPortMap(List<Port> ports) {
        Map<String, String> mimeTypeToPort = new HashMap<>();
        for (Port port : ports) {
            if (StringUtils.isBlank(port.mimeTypesCsv)) {
                log.warn("undefined mime types for port '" + port.name + "', no data will be dispatched to it");
                continue;
            }
            for (String mimeType : StringUtils.split(port.mimeTypesCsv, ',')) {
                String trimmed = mimeType.trim();
                if (!trimmed.isEmpty() && !UNDEFINED.equals(trimmed)) {
                    mimeTypeToPort.put(trimmed.toLowerCase(), port.name);
                }
            }
        }
        return mimeTypeToPort;
    }

    private static String resolvePortName(DocumentContentUrl record, Map<String, String> mimeTypeToPort) {
        if (record.getMimeType() == null) {
            log.warn("got null mime type for object: " + record.getId());
            return null;
        }
        String lowercasedMimeType = record.getMimeType().toString().toLowerCase();
        String portName = mimeTypeToPort.get(lowercasedMimeType);
        if (portName == null) {
            log.warn("skipping, got unhandled mime type: " + lowercasedMimeType
                    + " for object: " + record.getId());
        }
        return portName;
    }

    /**
     * Single output port definition.
     */
    static class Port implements Serializable {

        private static final long serialVersionUID = 1L;

        final String name;
        final String mimeTypesCsv;
        final String reportEntryKey;

        Port(String name, String mimeTypesCsv, String reportEntryKey) {
            this.name = name;
            this.mimeTypesCsv = mimeTypesCsv;
            this.reportEntryKey = reportEntryKey;
        }
    }

    //------------------------ PARAMETERS --------------------------

    @Parameters(separators = "=")
    public static class Params {

        @Parameter(names = "-input", required = true)
        public String input;

        @Parameter(names = "-outputRoot", required = true)
        public String outputRoot;

        @Parameter(names = "-outputReportPath")
        public String outputReportPath;

        @Parameter(names = "-outputNamePdf")
        public String outputNamePdf = "pdf";

        @Parameter(names = "-outputNameHtml")
        public String outputNameHtml = "html";

        @Parameter(names = "-outputNameXmlPmc")
        public String outputNameXmlPmc = "xmlpmc";

        @Parameter(names = "-outputNameWos")
        public String outputNameWos = "wos";

        @Parameter(names = "-mimetypesPdf", required = true)
        public String mimetypesPdf;

        @Parameter(names = "-mimetypesHtml", required = true)
        public String mimetypesHtml;

        @Parameter(names = "-mimetypesXmlPmc", required = true)
        public String mimetypesXmlPmc;

        @Parameter(names = "-mimetypesWos", required = true)
        public String mimetypesWos;

        @Parameter(names = "-reportEntryPdf")
        public String reportEntryPdf = "import.content.urls.bytype.pdf";

        @Parameter(names = "-reportEntryHtml")
        public String reportEntryHtml = "import.content.urls.bytype.html";

        @Parameter(names = "-reportEntryXmlPmc")
        public String reportEntryXmlPmc = "import.content.urls.bytype.jats";

        @Parameter(names = "-reportEntryWos")
        public String reportEntryWos = "import.content.urls.bytype.wos";

        List<Port> ports() {
            return Lists.newArrayList(
                    new Port(outputNamePdf, mimetypesPdf, normalize(reportEntryPdf)),
                    new Port(outputNameHtml, mimetypesHtml, normalize(reportEntryHtml)),
                    new Port(outputNameXmlPmc, mimetypesXmlPmc, normalize(reportEntryXmlPmc)),
                    new Port(outputNameWos, mimetypesWos, normalize(reportEntryWos)));
        }

        /**
         * Report entries are disabled when the key is not prefixed with the {@code report.}
         * marker conventions of the Oozie ReportGenerator; an undefined key disables reporting.
         */
        private static String normalize(String reportEntryKey) {
            return StringUtils.isBlank(reportEntryKey) || UNDEFINED.equals(reportEntryKey)
                    ? null : reportEntryKey;
        }
    }
}
