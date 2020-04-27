package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.report.pushgateway.converter.ReportEntryToMetricConverter;
import eu.dnetlib.iis.wf.report.pushgateway.pusher.MetricPusher;
import eu.dnetlib.iis.wf.report.pushgateway.pusher.MetricPusherCreator;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PushMetricsProcess implements Process {

    private static final Logger logger = LoggerFactory.getLogger(PushMetricsProcess.class);

    private static final String METRIC_PUSHER_CREATOR_CLASS_NAME = "metricPusherCreatorClassName";
    private static final String METRIC_PUSHER_ADDRESS = "metricPusherAddress";
    private static final String LABELED_METRIC = "labeledMetric";
    private static final String METRIC_LABEL_NAMES_SEP = ",";
    private static final String REPORT_LOCATION = "reportLocation";
    private static final String JOB_NAME = "jobName";

    private MetricPusherCreatorProducer metricPusherCreatorProducer = new MetricPusherCreatorProducer();
    private MetricPusherProducer metricPusherProducer = new MetricPusherProducer();
    private FileSystemProducer fileSystemProducer = new FileSystemProducer();
    private ReportLocationsFinder reportLocationsFinder = new ReportLocationsFinder();
    private LabelNamesByMetricNameProducer labelNamesByMetricNameProducer = new LabelNamesByMetricNameProducer();
    private ReportEntryReader reportEntryReader = new ReportEntryReader();
    private ReportEntryConverter reportEntryConverter = new ReportEntryConverter();
    private GaugesRegistrar gaugesRegistrar = new GaugesRegistrar();
    private JobNameProducer jobNameProducer = new JobNameProducer();

    @Override
    public Map<String, PortType> getInputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.emptyMap();
    }

    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) {
        metricPusherCreatorProducer.create(parameters)
                .ifPresent(metricPusherCreator -> {
                    logger.info("Using MetricPusherCreatorProducer: {}", metricPusherCreator);
                    metricPusherProducer.create(metricPusherCreator, parameters)
                            .ifPresent(metricPusher -> {
                                logger.info("Using MetricPusher: {}", metricPusher);
                                fileSystemProducer.create(conf)
                                        .ifPresent(fs -> {
                                            logger.info("Using FileSystem: {}", fs);
                                            reportLocationsFinder.find(parameters)
                                                    .ifPresent(
                                                            paths -> {
                                                                logger.info("Using report locations of size: {}", paths.size());
                                                                labelNamesByMetricNameProducer.create(parameters)
                                                                        .ifPresent(labelNamesByMetricName -> {
                                                                            logger.info("Using label map of size: {}", labelNamesByMetricName.size());
                                                                            List<Optional<List<Gauge>>> optionals = paths.stream()
                                                                                    .map(path -> reportEntryReader.read(fs, new Path(path))
                                                                                            .flatMap(reportEntries -> reportEntryConverter.convert(reportEntries, path, labelNamesByMetricName)))
                                                                                    .collect(Collectors.toList());
                                                                            unwrapOptionals(optionals)
                                                                                    .map(gauges -> gauges.stream().flatMap(Collection::stream).collect(Collectors.toList()))
                                                                                    .ifPresent(gauges -> {
                                                                                        logger.info("Using gauges of size: {}", gauges.size());
                                                                                        gaugesRegistrar.register(gauges)
                                                                                                .ifPresent(registry -> {
                                                                                                    logger.info("Using registry: {}", registry);
                                                                                                    jobNameProducer.create(parameters)
                                                                                                            .ifPresent(jobName -> {
                                                                                                                logger.info("Using job name: {}", jobName);
                                                                                                                metricPusher.pushSafe(registry, jobName);
                                                                                                            });
                                                                                                });
                                                                                    });
                                                                        });
                                                            });
                                        });
                            });
                });
    }

    private static <T> Optional<List<T>> unwrapOptionals(List<Optional<T>> optionals) {
        try {
            return Optional
                    .of(optionals.stream()
                            .map(optional -> optional.orElseThrow(RuntimeException::new))
                            .collect(Collectors.toList()));
        } catch (Exception e) {
            logger.error("Unwrapping of optionals failed.");
            e.printStackTrace();
            return Optional.empty();
        }
    }

    public static class MetricPusherCreatorProducer {
        Optional<MetricPusherCreator> create(Map<String, String> parameters) {
            Supplier<MetricPusherCreator> metricPusherCreatorSupplier = () -> {
                try {
                    return (MetricPusherCreator) Class.forName(parameters.get(METRIC_PUSHER_CREATOR_CLASS_NAME))
                            .getConstructor().newInstance();
                } catch (Exception e) {
                    logger.error("MetricPusherCreator creation failed.");
                    e.printStackTrace();
                    return null;
                }
            };
            return create(metricPusherCreatorSupplier);
        }

        Optional<MetricPusherCreator> create(Supplier<MetricPusherCreator> metricPusherCreatorSupplier) {
            return Optional.ofNullable(metricPusherCreatorSupplier.get());
        }
    }

    public static class MetricPusherProducer {
        Optional<MetricPusher> create(MetricPusherCreator metricPusherCreator, Map<String, String> parameters) {
            Supplier<MetricPusher> metricPusherSupplier = () -> {
                try {
                    return metricPusherCreator.create(parameters.get(METRIC_PUSHER_ADDRESS));
                } catch (Exception e) {
                    logger.error("MetricPusher creation failed.");
                    e.printStackTrace();
                    return null;
                }
            };
            return create(metricPusherSupplier);
        }

        Optional<MetricPusher> create(Supplier<MetricPusher> metricPusherSupplier) {
            return Optional.ofNullable(metricPusherSupplier.get());
        }
    }

    public static class FileSystemProducer {
        Optional<FileSystem> create(Configuration conf) {
            Supplier<FileSystem> fileSystemSupplier = () -> {
                try {
                    return FileSystem.get(conf);
                } catch (Exception e) {
                    logger.error("FileSystem creation failed.");
                    e.printStackTrace();
                    return null;
                }
            };
            return create(fileSystemSupplier);
        }

        Optional<FileSystem> create(Supplier<FileSystem> fileSystemSupplier) {
            return Optional.ofNullable(fileSystemSupplier.get());
        }
    }

    public static class ReportLocationsFinder {
        Optional<List<String>> find(Map<String, String> parameters) {
            Supplier<List<String>> reportLocationsSupplier = () -> {
                try {
                    return parameters.entrySet().stream()
                            .filter(entry -> isParameterReportLocation(entry.getKey()))
                            .map(Map.Entry::getValue)
                            .collect(Collectors.toList());
                } catch (Exception e) {
                    logger.error("Report locations finder failed.");
                    e.printStackTrace();
                    return null;
                }
            };
            return find(reportLocationsSupplier);
        }

        Optional<List<String>> find(Supplier<List<String>> reportLocationsSupplier) {
            return Optional
                    .ofNullable(reportLocationsSupplier.get())
                    .filter(paths -> paths.stream().noneMatch(StringUtils::isBlank))
                    .filter(CollectionUtils::isNotEmpty);
        }

        private static Boolean isParameterReportLocation(String parameterName) {
            return parameterName.startsWith(REPORT_LOCATION);
        }
    }

    public static class LabelNamesByMetricNameProducer {
        Optional<Map<String, String[]>> create(Map<String, String> parameters) {
            Supplier<Map<String, String[]>> labelNamesByMetricNameSupplier = () -> {
                try {
                    return parameters.entrySet().stream()
                            .filter(LabelNamesByMetricNameProducer::isParameterLabeledMetric)
                            .map(entry -> {
                                String metricName = metricNameOrThrow(entry.getKey());
                                String[] labelNames = labelNamesOrThrow(entry.getValue());
                                return new AbstractMap.SimpleEntry<>(metricName, labelNames);
                            })
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                } catch (Exception e) {
                    logger.error("Label names by metric name creation failed.");
                    e.printStackTrace();
                    return null;
                }
            };
            return create(labelNamesByMetricNameSupplier);
        }

        private static Boolean isParameterLabeledMetric(Map.Entry<String, String> entry) {
            return entry.getKey().startsWith(LABELED_METRIC);
        }

        private static String metricNameOrThrow(String parameterName) {
            return parameterName.substring(LABELED_METRIC.length() + 1);
        }

        private static String[] labelNamesOrThrow(String parameterValue) {
            String[] labelNames = parameterValue.split(METRIC_LABEL_NAMES_SEP);
            if (labelNames.length == 0 || StringUtils.isAnyBlank(labelNames)) {
                throw new RuntimeException("Label names creation failed.");
            }
            return labelNames;
        }

        Optional<Map<String, String[]>> create(Supplier<Map<String, String[]>> labelNamesByMetricNameSupplier) {
            return Optional.ofNullable(labelNamesByMetricNameSupplier.get());
        }
    }

    public static class ReportEntryReader {
        Optional<List<ReportEntry>> read(FileSystem fs, Path path) {
            Supplier<List<ReportEntry>> reportEntriesSupplier = () -> {
                try {
                    return DataStore.read(new FileSystemPath(fs, path), ReportEntry.SCHEMA$);
                } catch (Exception e) {
                    logger.error("Reading data store failed.");
                    e.printStackTrace();
                    return null;
                }
            };
            return read(reportEntriesSupplier);
        }

        Optional<List<ReportEntry>> read(Supplier<List<ReportEntry>> reportEntriesSupplier) {
            return Optional
                    .ofNullable(reportEntriesSupplier.get())
                    .filter(CollectionUtils::isNotEmpty);
        }
    }

    public static class ReportEntryConverter {
        Optional<List<Gauge>> convert(List<ReportEntry> reportEntries, String path, Map<String, String[]> labelNamesByMetricName) {
            Supplier<List<Gauge>> gaugesSupplier = () -> {
                try {
                    return ReportEntryToMetricConverter.convert(reportEntries, path, labelNamesByMetricName);
                } catch (Exception e) {
                    logger.error("Report entries conversion failed.");
                    e.printStackTrace();
                    return null;
                }
            };
            return convert(gaugesSupplier);
        }

        Optional<List<Gauge>> convert(Supplier<List<Gauge>> gaugesSupplier) {
            return Optional
                    .ofNullable(gaugesSupplier.get())
                    .filter(CollectionUtils::isNotEmpty);
        }
    }

    public static class GaugesRegistrar {
        Optional<CollectorRegistry> register(List<Gauge> gauges) {
            return register(gauges, CollectorRegistry::new);
        }

        Optional<CollectorRegistry> register(List<Gauge> gauges, Supplier<CollectorRegistry> collectorRegistryCreator) {
            Supplier<CollectorRegistry> collectorRegistrySupplier = () -> {
                try {
                    CollectorRegistry collectorRegistry = collectorRegistryCreator.get();
                    gauges.forEach(collectorRegistry::register);
                    return collectorRegistry;
                } catch (Exception e) {
                    logger.error("Gauges registration failed.");
                    e.printStackTrace();
                    return null;
                }
            };
            return register(collectorRegistrySupplier);
        }

        Optional<CollectorRegistry> register(Supplier<CollectorRegistry> collectorRegistrySupplier) {
            return Optional.ofNullable(collectorRegistrySupplier.get());
        }
    }

    public static class JobNameProducer {
        Optional<String> create(Map<String, String> parameters) {
            Supplier<String> jobNameSupplier = () -> {
                try {
                    return parameters.get(JOB_NAME);
                } catch (Exception e) {
                    logger.error("Job name extraction failed.");
                    e.printStackTrace();
                    return null;
                }
            };
            return create(jobNameSupplier);
        }

        Optional<String> create(Supplier<String> jobNameSupplier) {
            return Optional
                    .ofNullable(jobNameSupplier.get())
                    .filter(StringUtils::isNotBlank);
        }
    }

}
