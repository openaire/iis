package eu.dnetlib.iis.wf.report.pushgateway.process;

import eu.dnetlib.iis.common.ClassPathResourceProvider;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.io.HdfsUtils;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.common.schemas.ReportEntry;
import eu.dnetlib.iis.wf.report.pushgateway.converter.LabeledMetricConf;
import eu.dnetlib.iis.wf.report.pushgateway.converter.ReportEntryToMetricConverter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Pushes metrics created from report entries to an instance of pushgateway.
 * <p>
 * Supports a read from a single report dir location, conversion to prometheus gauges with or without labels and a push to
 * running instance of pushgateway. Gauges with labels are created for report entries with keys matching regex patterns in
 * labeled metrics properties file. Any grouping keys may be supplied for the process. Any exceptions during any stage of
 * the process will not result in a failed process.
 */
public class PushMetricsProcess implements Process {

    private static final Logger logger = LoggerFactory.getLogger(PushMetricsProcess.class);

    private static final String METRIC_PUSHER_CREATOR_CLASS_NAME = "metricPusherCreatorClassName";
    private static final String METRIC_PUSHER_ADDRESS = "metricPusherAddress";
    private static final String REPORTS_DIR_PATH = "reportsDirPath";
    private static final String LABELED_METRICS_PROPERTIES_FILE = "labeledMetricsPropertiesFile";
    private static final String GROUPING_KEY = "groupingKey";
    private static final String JOB = "iis";

    private MetricPusherCreatorProducer metricPusherCreatorProducer = new MetricPusherCreatorProducer();
    private MetricPusherProducer metricPusherProducer = new MetricPusherProducer();
    private FileSystemProducer fileSystemProducer = new FileSystemProducer();
    private ReportLocationsFinder reportLocationsFinder = new ReportLocationsFinder();
    private LabeledMetricConfByPatternProducer labeledMetricConfByPatternProducer = new LabeledMetricConfByPatternProducer();
    private ReportEntryReader reportEntryReader = new ReportEntryReader();
    private ReportEntryConverter reportEntryConverter = new ReportEntryConverter();
    private GaugesRegistrar gaugesRegistrar = new GaugesRegistrar();
    private GroupingKeyProducer groupingKeyProducer = new GroupingKeyProducer();

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
                                                    .ifPresent(paths -> {
                                                        logger.info("Using report locations of size: {}", paths.size());
                                                        labeledMetricConfByPatternProducer.create(parameters)
                                                                .ifPresent(labeledMetricConfByPattern -> {
                                                                    logger.info("Using label map of size: {}", labeledMetricConfByPattern.size());
                                                                    List<Optional<List<ReportEntry>>> collect = paths.stream()
                                                                            .map(path -> reportEntryReader.read(fs, new Path(path)))
                                                                            .collect(Collectors.toList());
                                                                    unwrapOptionals(collect)
                                                                            .map(x -> x.stream().flatMap(Collection::stream).collect(Collectors.toList()))
                                                                            .ifPresent(reportEntries -> {
                                                                                logger.info("Using report entries of size: {}", reportEntries.size());
                                                                                reportEntryConverter.convert(reportEntries, parameters.get(REPORTS_DIR_PATH), labeledMetricConfByPattern)
                                                                                        .ifPresent(gauges -> {
                                                                                            logger.info("Using gauges of size: {}", gauges.size());
                                                                                            gaugesRegistrar.register(gauges)
                                                                                                    .ifPresent(registry -> {
                                                                                                        logger.info("Using registry: {}", registry);
                                                                                                        groupingKeyProducer.create(parameters)
                                                                                                                .ifPresent(groupingKey -> {
                                                                                                                    logger.info("Using grouping key of size: {}", groupingKey.size());
                                                                                                                    metricPusher.pushSafe(registry, JOB, groupingKey);
                                                                                                                });
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
                            .map(optional -> optional.orElse(null))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList()))
                    .filter(CollectionUtils::isNotEmpty);
        } catch (Exception e) {
            logger.error("Unwrapping of optionals failed.", e);
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
                    logger.error("MetricPusherCreator creation failed.", e);
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
                    logger.error("MetricPusher creation failed.", e);
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
                    logger.error("FileSystem creation failed.", e);
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
                    return HdfsUtils.listDirs(Job.getInstance().getConfiguration(), parameters.get(REPORTS_DIR_PATH));
                } catch (Exception e) {
                    logger.error("Report locations finder failed.", e);
                    return null;
                }
            };
            return find(reportLocationsSupplier);
        }

        Optional<List<String>> find(Supplier<List<String>> reportLocationsSupplier) {
            return Optional
                    .ofNullable(reportLocationsSupplier.get())
                    .filter(CollectionUtils::isNotEmpty)
                    .filter(paths -> paths.stream().noneMatch(StringUtils::isBlank));
        }
    }

    public static class LabeledMetricConfByPatternProducer {
        private static final ObjectMapper objectMapper = new ObjectMapper();

        Optional<Map<String, LabeledMetricConf>> create(Map<String, String> parameters) {
            Supplier<Map<String, LabeledMetricConf>> labelNamesByMetricNameSupplier = () -> {
                try {
                    String labeledMetricsPropertiesFile = parameters.get(LABELED_METRICS_PROPERTIES_FILE);
                    Properties labeledMetricsProperties = new Properties();
                    labeledMetricsProperties.load(ClassPathResourceProvider.getResourceInputStream(labeledMetricsPropertiesFile));
                    return labeledMetricsProperties.entrySet().stream()
                            .map(entry -> {
                                String metricPattern = (String) entry.getKey();
                                String labeledMetricConfJson = (String) entry.getValue();
                                LabeledMetricConf labeledMetricConf = labeledMetricConfOrThrow(labeledMetricConfJson);
                                return new AbstractMap.SimpleEntry<>(metricPattern, labeledMetricConf);
                            })
                            .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
                } catch (Exception e) {
                    logger.error("Label names by metric name creation failed.", e);
                    return null;
                }
            };
            return create(labelNamesByMetricNameSupplier);
        }

        private static LabeledMetricConf labeledMetricConfOrThrow(String json) {
            try {
                return objectMapper.readValue(json, LabeledMetricConf.class);
            } catch (IOException e) {
                throw new RuntimeException(String.format("failed to parse label metric conf: json=%s", json), e);
            }
        }

        Optional<Map<String, LabeledMetricConf>> create(Supplier<Map<String, LabeledMetricConf>> labeledMetricConfByPatternSupplier) {
            return Optional
                    .ofNullable(labeledMetricConfByPatternSupplier.get())
                    .filter(MapUtils::isNotEmpty)
                    .filter(map -> map.entrySet().stream().noneMatch(entry -> entry.getKey().isEmpty() || Objects.isNull(entry.getValue())));
        }
    }

    public static class ReportEntryReader {
        Optional<List<ReportEntry>> read(FileSystem fs, Path path) {
            Supplier<List<ReportEntry>> reportEntriesSupplier = () -> {
                try {
                    return DataStore.read(new FileSystemPath(fs, path), ReportEntry.SCHEMA$);
                } catch (Exception e) {
                    logger.error("Reading data store failed.", e);
                    return null;
                }
            };
            return read(reportEntriesSupplier);
        }

        Optional<List<ReportEntry>> read(Supplier<List<ReportEntry>> reportEntriesSupplier) {
            return Optional
                    .ofNullable(reportEntriesSupplier.get())
                    .filter(CollectionUtils::isNotEmpty)
                    .filter(entries -> entries.stream().noneMatch(Objects::isNull));
        }
    }

    public static class ReportEntryConverter {
        Optional<List<Gauge>> convert(List<ReportEntry> reportEntries, String help, Map<String, LabeledMetricConf> labeledMetricConfByPattern) {
            Supplier<List<Gauge>> gaugesSupplier = () -> {
                try {
                    return ReportEntryToMetricConverter.convert(reportEntries, help, labeledMetricConfByPattern);
                } catch (Exception e) {
                    logger.error("Report entries conversion failed.", e);
                    return null;
                }
            };
            return convert(gaugesSupplier);
        }

        Optional<List<Gauge>> convert(Supplier<List<Gauge>> gaugesSupplier) {
            return Optional
                    .ofNullable(gaugesSupplier.get())
                    .filter(CollectionUtils::isNotEmpty)
                    .filter(gauges -> gauges.stream().noneMatch(Objects::isNull));
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
                    logger.error("Gauges registration failed.", e);
                    return null;
                }
            };
            return register(collectorRegistrySupplier);
        }

        Optional<CollectorRegistry> register(Supplier<CollectorRegistry> collectorRegistrySupplier) {
            return Optional.ofNullable(collectorRegistrySupplier.get());
        }
    }

    public static class GroupingKeyProducer {
        Optional<Map<String, String>> create(Map<String, String> parameters) {
            Supplier<Map<String, String>> groupingKeySupplier = () -> {
                try {
                    return parameters.entrySet().stream()
                            .filter(GroupingKeyProducer::isParameterGroupingKey)
                            .map(entry -> {
                                String key = keyOrThrow(entry.getKey());
                                String value = entry.getValue();
                                return new AbstractMap.SimpleEntry<>(key, value);
                            })
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                } catch (Exception e) {
                    logger.error("Grouping key extraction failed.", e);
                    return null;
                }
            };
            return create(groupingKeySupplier);
        }

        private static Boolean isParameterGroupingKey(Map.Entry<String, String> entry) {
            return entry.getKey().startsWith(GROUPING_KEY);
        }

        private static String keyOrThrow(String parameterName) {
            return parameterName.substring(GROUPING_KEY.length() + 1);
        }

        Optional<Map<String, String>> create(Supplier<Map<String, String>> groupingKeySupplier) {
            return Optional
                    .ofNullable(groupingKeySupplier.get())
                    .filter(MapUtils::isNotEmpty)
                    .filter(groupingKey -> groupingKey.entrySet().stream()
                            .noneMatch(entry -> StringUtils.isBlank(entry.getKey()) || StringUtils.isBlank(entry.getValue())));
        }
    }

}
