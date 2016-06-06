package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_SETID;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ALGORITHM_PROPERTY_SEPARATOR;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.module.ActionBuilderFactory;
import eu.dnetlib.iis.wf.export.actionmanager.module.ActionBuilderModule;
import eu.dnetlib.iis.wf.export.actionmanager.module.AlgorithmName;
import eu.dnetlib.iis.wf.export.actionmanager.module.MappingNotDefinedException;
import eu.dnetlib.iis.wf.export.actionmanager.module.TrustLevelThresholdExceededException;

/**
 * ActionManager service based exporter mapper.
 * 
 * @author mhorst
 *
 */
public class SequenceFileExporterMapper extends Mapper<AvroKey<? extends SpecificRecordBase>, NullWritable, Text, Text> {

    private ActionBuilderModule<SpecificRecordBase> actionBuilder;

    
    // ----------------------- LOGIC --------------------------------
    
    /** This is the place you can access map-reduce workflow node parameters */
    @SuppressWarnings("unchecked")
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String moduleClassName = context.getConfiguration().get(EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME);
        if (StringUtils.isNotBlank(moduleClassName)) {
            try {
                Class<?> clazz = Class.forName(moduleClassName);
                Constructor<?> constructor = clazz.getConstructor();
                ActionBuilderFactory<SpecificRecordBase> actionBuilderFactory = (ActionBuilderFactory<SpecificRecordBase>) constructor.newInstance();
                actionBuilder = actionBuilderFactory.instantiate(context.getConfiguration(), StaticConfigurationProvider.AGENT_DEFAULT,
                        getActionSetId(actionBuilderFactory.getAlgorithName(), context.getConfiguration()));
            } catch (Exception e) {
                throw new RuntimeException(
                        "unexpected exception ocurred when instantiating " + "builder module: " + moduleClassName, e);
            }
        } else {
            throw new RuntimeException("unknown action builder module instance, " + "no "
                    + EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME + " parameter provided!");
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
    
    // ----------------------- PRIVATE --------------------------------
    
    /**
     * Provides action set identifier extracted from job configuration.
     * Checks whether action set was defined for particular algorithm or picks default value if specified.
     * @param algorithmName inference algorithm name
     * @param cfg job configuration
     * @throws MappingNotDefinedException thrown when action set identifier not specified in configuration
     */
    private static String getActionSetId(AlgorithmName algorithmName, Configuration cfg) throws MappingNotDefinedException {
        String actionSetId = WorkflowRuntimeParameters.getParamValue(
                EXPORT_ACTION_SETID + EXPORT_ALGORITHM_PROPERTY_SEPARATOR + algorithmName.name(), 
                EXPORT_ACTION_SETID, cfg);
        if (actionSetId!=null) {
            return actionSetId;
        } else {
            throw new MappingNotDefinedException(
                    "no action set identifier defined " + "for algorithm: " + algorithmName.name());
        }
    }

    /**
     * Creates list of actions for given avro object.
     * 
     * @param datum source avro object
     */
    private List<AtomicAction> createActions(SpecificRecordBase datum) {
        try {
            return actionBuilder.build(datum);
        } catch (TrustLevelThresholdExceededException e) {
            return Collections.emptyList();
        }
    }

}