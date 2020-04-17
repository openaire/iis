package eu.dnetlib.iis.wf.export.actionmanager.sequencefile;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.List;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.iis.wf.export.actionmanager.ActionSerializationUtils;
import eu.dnetlib.iis.wf.export.actionmanager.module.ActionBuilderFactory;
import eu.dnetlib.iis.wf.export.actionmanager.module.ActionBuilderModule;
import eu.dnetlib.iis.wf.export.actionmanager.module.TrustLevelThresholdExceededException;

/**
 * ActionManager service based exporter mapper.
 * 
 * @author mhorst
 *
 */
public class SequenceFileExporterMapper extends Mapper<AvroKey<? extends SpecificRecordBase>, NullWritable, Text, Text> {

    private ActionBuilderModule<SpecificRecordBase, Oaf> actionBuilder;

    private ObjectMapper objectMapper;
    
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
                ActionBuilderFactory<SpecificRecordBase, Oaf> actionBuilderFactory = (ActionBuilderFactory<SpecificRecordBase, Oaf>) constructor.newInstance();
                actionBuilder = actionBuilderFactory.instantiate(context.getConfiguration());
                objectMapper = new ObjectMapper();
            } catch (Exception e) {
                throw new RuntimeException("unexpected exception ocurred when instantiating " + "builder module: " + moduleClassName, e);
            }
        } else {
            throw new InvalidParameterException("unknown action builder module instance, " + "no "
                    + EXPORT_ACTION_BUILDER_FACTORY_CLASSNAME + " parameter provided!");
        }
    }

    @Override
    protected void map(AvroKey<? extends SpecificRecordBase> key, NullWritable ignore, Context context)
            throws IOException, InterruptedException {
        List<AtomicAction<Oaf>> actions = createActions(key.datum());
        if (actions != null) {
            for (AtomicAction<Oaf> action : actions) {
                Text keyOut = new Text();
                Text valueOut = new Text();
                keyOut.set("");
                valueOut.set(ActionSerializationUtils.serializeAction(action, objectMapper));
                context.write(keyOut, valueOut);
            }
        }
    }
    
    // ----------------------- PRIVATE --------------------------------

    /**
     * Creates list of actions for given avro object.
     * 
     * @param datum source avro object
     */
    private List<AtomicAction<Oaf>> createActions(SpecificRecordBase datum) {
        try {
            return actionBuilder.build(datum);
        } catch (TrustLevelThresholdExceededException e) {
            return Collections.emptyList();
        }
    }

}