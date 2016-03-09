package eu.dnetlib.iis.wf.export.actionmanager;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ALGORITHM_PROPERTY_SEPARATOR;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_TRUST_LEVEL;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_TRUST_LEVEL_THRESHOLD;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.rmi.ActionManagerException;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.wf.export.actionmanager.api.ActionManagerServiceFacade;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.ActionManagerConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.module.ActionBuilderFactory;
import eu.dnetlib.iis.wf.export.actionmanager.module.ActionBuilderModule;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmMapper;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.TrustLevelThresholdExceededException;

/**
 * ActionManager service based exporter mapper.
 * @author mhorst
 *
 */
public abstract class AbstractActionManagerBasedExporterMapper 
extends Mapper<AvroKey<? extends SpecificRecordBase>, NullWritable, NullWritable, NullWritable> {
	
	private String predefinedTrust = "0.9";
	
	private ActionManagerServiceFacade actionManager;
	
	private ActionManagerConfigurationProvider configProvider;
	
	private ActionBuilderFactory<SpecificRecordBase> actionBuilderFactory;
	
	private ActionBuilderModule<SpecificRecordBase> actionBuilder;
	
	private AlgorithmMapper<String> actionSetIdProvider;

	
	/** This is the place you can access map-reduce workflow node parameters */
	@SuppressWarnings("unchecked")
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		if (context.getConfiguration().get(
				EXPORT_TRUST_LEVEL)!=null) {
			this.predefinedTrust = context.getConfiguration().get(
					EXPORT_TRUST_LEVEL);
		}

		String moduleClassName = context.getConfiguration().get(
				EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME);
		if (moduleClassName!=null) {
			try {
				actionSetIdProvider = AlgorithmToActionSetMapper.instantiate(
						context.getConfiguration());
				Class<?> clazz = Class.forName(moduleClassName);
				Constructor<?> constructor = clazz.getConstructor();
				actionBuilderFactory = (ActionBuilderFactory<SpecificRecordBase>) constructor.newInstance();
				actionBuilder = actionBuilderFactory.instantiate(
						predefinedTrust, provideTrustLevelThreshold(
								context, actionBuilderFactory.getAlgorithName()), 
						context.getConfiguration());
				actionManager = buildActionManager(context);
				configProvider = new StaticConfigurationProvider(
						StaticConfigurationProvider.AGENT_DEFAULT,
						StaticConfigurationProvider.PROVENANCE_DEFAULT,
						StaticConfigurationProvider.ACTION_TRUST_0_9,
						StaticConfigurationProvider.NAMESPACE_PREFIX_DEFAULT);
				
			} catch (Exception e) {
				throw new RuntimeException(
						"unexpected exception ocurred when instantiating "
						+ "builder module: " + moduleClassName, e);
			} 
		} else {
			throw new RuntimeException("unknown action builder module instance, "
					+ "no " + EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME + 
					" parameter provided!");
		}
	}

	/**
	 * Provides trust level threshold if defined for given algorithm or globally.
	 * @param context
	 * @param algorithmName
	 * @return trust level threshold or null if not defined
	 */
	private Float provideTrustLevelThreshold(Context context, AlgorithmName algorithmName) {
		String algorithmTrustLevelThreshold = context.getConfiguration().get(
				EXPORT_TRUST_LEVEL_THRESHOLD + EXPORT_ALGORITHM_PROPERTY_SEPARATOR + 
				algorithmName.name());
		if (algorithmTrustLevelThreshold!=null && 
				!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(
						algorithmTrustLevelThreshold)) {
			return Float.valueOf(algorithmTrustLevelThreshold);	
		}
		String defaultTrustLevelThresholdStr = context.getConfiguration().get(
				EXPORT_TRUST_LEVEL_THRESHOLD);
		if (defaultTrustLevelThresholdStr!=null && 
				!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(
						defaultTrustLevelThresholdStr)) {
			return Float.valueOf(defaultTrustLevelThresholdStr);	
		}
//		fallback: threshold was not defined
		return null;
	}
	
	
	
	/**
	 * Builds action manager instance.
	 * @param context
	 * @throws IOException
	 * @return action manager instance
	 */
	protected abstract ActionManagerServiceFacade buildActionManager(Context context) throws IOException;
	
	/**
	 * Creates collection of actions for given datum
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
	protected void map(AvroKey<? extends SpecificRecordBase> key, NullWritable ignore, 
			Context context) throws IOException, InterruptedException {
		try {
			List<AtomicAction> actions = createActions(key.datum());
			if (actions!=null) {
					actionManager.storeAction( 
							actions,
							null, 
							null, 
							configProvider.provideNamespacePrefix());	
			}
		} catch (ActionManagerException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		try {
			if (actionManager!=null) {
				actionManager.close();	
			}
		} catch (ActionManagerException e) {
			throw new IOException(e);
		} finally {
			super.cleanup(context);
		}
	}
	
}