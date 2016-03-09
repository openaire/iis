package eu.dnetlib.iis.workflows.export.actionmanager.sequencefile;

import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME;
import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_SETID;
import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ALGORITHM_PROPERTY_SEPARATOR;
import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_TRUST_LEVEL;
import static eu.dnetlib.iis.workflows.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_TRUST_LEVEL_THRESHOLD;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.workflows.export.actionmanager.cfg.ActionManagerConfigurationProvider;
import eu.dnetlib.iis.workflows.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.workflows.export.actionmanager.module.ActionBuilderFactory;
import eu.dnetlib.iis.workflows.export.actionmanager.module.ActionBuilderModule;
import eu.dnetlib.iis.workflows.export.actionmanager.module.AlgorithmMapper;
import eu.dnetlib.iis.workflows.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.workflows.export.actionmanager.module.MappingNotDefinedException;
import eu.dnetlib.iis.workflows.export.actionmanager.module.TrustLevelThresholdExceededException;

/**
 * ActionManager service based exporter mapper.
 * 
 * @author mhorst
 *
 */
public class SequenceFileExporterMapper
		extends Mapper<AvroKey<? extends SpecificRecordBase>, NullWritable, Text, Text> {

	private String predefinedTrust = "0.9";

	private ActionManagerConfigurationProvider configProvider;

	private ActionBuilderFactory<SpecificRecordBase> actionBuilderFactory;

	private ActionBuilderModule<SpecificRecordBase> actionBuilder;

	private AlgorithmMapper<String> actionSetIdProvider;
	
	/** This is the place you can access map-reduce workflow node parameters */
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		if (context.getConfiguration().get(EXPORT_TRUST_LEVEL) != null) {
			this.predefinedTrust = context.getConfiguration().get(EXPORT_TRUST_LEVEL);
		}

		String moduleClassName = context.getConfiguration().get(EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME);
		if (moduleClassName != null) {
			try {
				actionSetIdProvider = provideAlgorithmToActionSetMapper(context);
				Class<?> clazz = Class.forName(moduleClassName);
				Constructor<?> constructor = clazz.getConstructor();
				actionBuilderFactory = (ActionBuilderFactory<SpecificRecordBase>) constructor.newInstance();
				actionBuilder = actionBuilderFactory.instantiate(predefinedTrust,
						provideTrustLevelThreshold(context, actionBuilderFactory.getAlgorithName()),
						context.getConfiguration());
				configProvider = new StaticConfigurationProvider(StaticConfigurationProvider.AGENT_DEFAULT,
						StaticConfigurationProvider.PROVENANCE_DEFAULT, StaticConfigurationProvider.ACTION_TRUST_0_9,
						StaticConfigurationProvider.NAMESPACE_PREFIX_DEFAULT);

			} catch (Exception e) {
				throw new RuntimeException(
						"unexpected exception ocurred when instantiating " + "builder module: " + moduleClassName, e);
			}
		} else {
			throw new RuntimeException("unknown action builder module instance, " + "no "
					+ EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME + " parameter provided!");
		}
	}

	/**
	 * Provides trust level threshold if defined for given algorithm or
	 * globally.
	 * 
	 * @param context
	 * @param algorithmName
	 * @return trust level threshold or null if not defined
	 */
	private static Float provideTrustLevelThreshold(Context context, AlgorithmName algorithmName) {
		String algorithmTrustLevelThreshold = context.getConfiguration()
				.get(EXPORT_TRUST_LEVEL_THRESHOLD + EXPORT_ALGORITHM_PROPERTY_SEPARATOR + algorithmName.name());
		if (algorithmTrustLevelThreshold != null
				&& !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(algorithmTrustLevelThreshold)) {
			return Float.valueOf(algorithmTrustLevelThreshold);
		}
		String defaultTrustLevelThresholdStr = context.getConfiguration().get(EXPORT_TRUST_LEVEL_THRESHOLD);
		if (defaultTrustLevelThresholdStr != null
				&& !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(defaultTrustLevelThresholdStr)) {
			return Float.valueOf(defaultTrustLevelThresholdStr);
		}
		// fallback: threshold was not defined
		return null;
	}

	private static AlgorithmMapper<String> provideAlgorithmToActionSetMapper(Context context) {
		String defaultActionSetId = null;
		if (context.getConfiguration().get(EXPORT_ACTION_SETID) != null
				&& !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE
						.equals(context.getConfiguration().get(EXPORT_ACTION_SETID))) {
			defaultActionSetId = context.getConfiguration().get(EXPORT_ACTION_SETID);
		}
		final Map<AlgorithmName, String> algoToActionSetMap = new HashMap<AlgorithmName, String>();
		for (AlgorithmName currentAlgorithm : AlgorithmName.values()) {
			String propertyKey = EXPORT_ACTION_SETID + EXPORT_ALGORITHM_PROPERTY_SEPARATOR + currentAlgorithm.name();
			if (context.getConfiguration().get(propertyKey) != null
					&& !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE
							.equals(context.getConfiguration().get(propertyKey))) {
				algoToActionSetMap.put(currentAlgorithm, context.getConfiguration().get(propertyKey));
			} else {
				algoToActionSetMap.put(currentAlgorithm, defaultActionSetId);
			}
		}
		return new AlgorithmMapper<String>() {
			@Override
			public String getValue(AlgorithmName algorithmName) throws MappingNotDefinedException {
				String actionSetId = algoToActionSetMap.get(algorithmName);
				if (actionSetId != null && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(actionSetId)) {
					return actionSetId;
				} else {
					throw new MappingNotDefinedException(
							"no action set identifier defined " + "for algorithm: " + algorithmName.name());
				}
			}
		};
	}

	/**
	 * Creates collection of actions for given datum
	 * 
	 * @param datum
	 * @return personBuilder of actions
	 */
	protected List<AtomicAction> createActions(SpecificRecordBase datum) {
		try {
			return actionBuilder.build(datum, configProvider.provideAgent(),
					actionSetIdProvider.getValue(actionBuilderFactory.getAlgorithName()));
		} catch (TrustLevelThresholdExceededException e) {
			return null;
		}
	}

	@Override
	protected void map(AvroKey<? extends SpecificRecordBase> key, NullWritable ignore, Context context)
			throws IOException, InterruptedException {
		List<AtomicAction> actions = createActions(key.datum());
		if (actions != null) {
			for (AtomicAction action : actions) {
				Text keyOut = new Text();
				Text valueOut = new Text();
				keyOut.set(action.getRowKey());
				valueOut.set(action.toString());
				context.write(keyOut, valueOut);
			}
		}
	}

}