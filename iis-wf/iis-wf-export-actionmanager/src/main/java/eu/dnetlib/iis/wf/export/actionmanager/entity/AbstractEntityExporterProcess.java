package eu.dnetlib.iis.wf.export.actionmanager.entity;

import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_ACTION_SETID;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_SEQ_FILE_OUTPUT_DIR_NAME;
import static eu.dnetlib.iis.wf.export.actionmanager.ExportWorkflowRuntimeParameters.EXPORT_SEQ_FILE_OUTPUT_DIR_ROOT;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import eu.dnetlib.actionmanager.actions.ActionFactory;
import eu.dnetlib.actionmanager.actions.AtomicAction;
import eu.dnetlib.actionmanager.actions.XsltInfoPackageAction;
import eu.dnetlib.actionmanager.common.Operation;
import eu.dnetlib.actionmanager.common.Provenance;
import eu.dnetlib.data.mdstore.DocumentNotFoundException;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.ProcessUtils;
import eu.dnetlib.iis.common.java.io.CloseableIterator;
import eu.dnetlib.iis.common.java.io.DataStore;
import eu.dnetlib.iis.common.java.io.FileSystemPath;
import eu.dnetlib.iis.common.java.porttype.AvroPortType;
import eu.dnetlib.iis.common.java.porttype.PortType;
import eu.dnetlib.iis.wf.export.actionmanager.api.ActionManagerServiceFacade;
import eu.dnetlib.iis.wf.export.actionmanager.api.SequenceFileActionManagerServiceFacade;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;
import eu.dnetlib.iis.wf.export.actionmanager.entity.facade.MDStoreFacade;
import eu.dnetlib.iis.wf.export.actionmanager.entity.facade.MDStoreFacadeFactory;

/**
 * Common codebase responsible for exporting generic entities. 
 * To be extended by classes responsible for exporting specific entity types. 
 * 
 * @author mhorst
 *
 */
public abstract class AbstractEntityExporterProcess<T extends SpecificRecordBase> implements Process {

    private static final String MDSTORE_FACADE_FACTORY_CLASS = "mdstore.facade.factory.classname";

    private static final Provenance PROVENANCE_DEFAULT = Provenance.sysimport_mining_repository;

    private final Logger log = Logger.getLogger(this.getClass());

    private final static String inputPort = "input";

    private final Schema inputPortSchema;

    private final String entityXSLTName;

    private final String entityXSLTLocation;

    private final String entityNamespacePrefix;
    
    private final ActionFactory actionFactory;

    // ------------------------ CONSTRUCTORS -----------------------------

    /**
     * @param inputPortSchema input port avro schema
     * @param entityXSLTName entity XSL transformation name
     * @param entityXSLTLocation entity XSL transformation location
     * @param entityNamespacePrefix namespace prefix to be used with given entity type
     */
    public AbstractEntityExporterProcess(Schema inputPortSchema, String entityXSLTName, String entityXSLTLocation,
            String entityNamespacePrefix) {
        this.inputPortSchema = inputPortSchema;
        this.entityXSLTName = entityXSLTName;
        this.entityXSLTLocation = entityXSLTLocation;
        this.entityNamespacePrefix = entityNamespacePrefix;
        this.actionFactory = buildActionFactory();

    }

    // ------------------------ LOGIC -----------------------------

    /**
     * @param portBindings input port name bound to HDFS location
     * @param conf hadoop configuration
     * @param parameters process parameters configuring action manager and mdstore facade
     */
    @Override
    public void run(PortBindings portBindings, Configuration conf, Map<String, String> parameters) throws Exception {
        String actionSetId = ProcessUtils.getParameterValue(EXPORT_ACTION_SETID, conf, parameters);
        Preconditions.checkArgument(StringUtils.isNotBlank(actionSetId) && !WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(actionSetId),
                "unable to export document entities to action manager due to missing action set identifier, "
                        + "no '%s' required parameter provided!", EXPORT_ACTION_SETID);

        FileSystemPath inputPath = new FileSystemPath(FileSystem.get(conf), portBindings.getInput().get(inputPort));

        try (ActionManagerServiceFacade actionManager = buildActionManager(conf, parameters);
                CloseableIterator<T> it = DataStore.<T> getReader(inputPath)) {
            
            MDStoreFacade mdStore = buildMDStoreFacade(parameters);

            int counter = 0;
            while (it.hasNext()) {
                MDStoreIdWithEntityId mdStoreComplexId = convertIdentifier(it.next());
                String mdRecordId = convertToMDStoreEntityId(mdStoreComplexId.getEntityId());
                try {
                    String mdRecord = mdStore.fetchRecord(mdStoreComplexId.getMdStoreId(), mdRecordId);
                    handleRecord(mdRecord, actionSetId, actionManager);
                    counter++;
                } catch (DocumentNotFoundException e) {
                    log.error("mdrecord: " + mdRecordId + " wasn't found in mdstore: "
                            + mdStoreComplexId.getMdStoreId(), e);
                } catch (Exception e) {
                    log.error("got exception when trying to retrieve " + "MDStore record for mdstore id "
                            + mdStoreComplexId.getMdStoreId() + ", and document id: " + mdRecordId, e);
                    throw e;
                }

            }
            log.warn("exported " + counter + " entities in total");
        }
    }

    @Override
    public Map<String, PortType> getInputPorts() {
        HashMap<String, PortType> inputPorts = Maps.newHashMap();
        inputPorts.put(inputPort, new AvroPortType(inputPortSchema));
        return inputPorts;
    }

    @Override
    public Map<String, PortType> getOutputPorts() {
        return Collections.emptyMap();
    }

    /**
     * Converts specific identifier object into generic representation with mdStore and entity identifiers explicitly set.
     * 
     * @return mdstore and entity identifiers pair
     */
    abstract protected MDStoreIdWithEntityId convertIdentifier(T element);

    
    // ------------------------ PRIVATE -----------------------------

    
    /**
     * Handles single record retrieved from MDStore.
     * 
     * @param mdStoreRecord MDStore record to be processed
     * @param actionSetId action set identifier to be attached to generated actions
     * @param actionManager generated actions consumer
     */
    private void handleRecord(String mdStoreRecord, String actionSetId, ActionManagerServiceFacade actionManager) throws Exception {

        XsltInfoPackageAction xsltAction = actionFactory.generateInfoPackageAction(entityXSLTName, actionSetId,
                StaticConfigurationProvider.AGENT_DEFAULT, Operation.INSERT, mdStoreRecord, PROVENANCE_DEFAULT,
                entityNamespacePrefix, StaticConfigurationProvider.ACTION_TRUST_0_9);
        
        List<AtomicAction> atomicActions = xsltAction.asAtomicActions();
        
        actionManager.storeActions(atomicActions);
    }

    /**
     * Creates action manager instance.
     * 
     * @param conf
     * @param parameters
     * @return action manager instance
     * @throws IOException
     */
    private ActionManagerServiceFacade buildActionManager(Configuration conf, Map<String, String> parameters)
            throws IOException {
        return new SequenceFileActionManagerServiceFacade(conf,
                ProcessUtils.getParameterValue(EXPORT_SEQ_FILE_OUTPUT_DIR_ROOT, conf, parameters),
                ProcessUtils.getParameterValue(EXPORT_SEQ_FILE_OUTPUT_DIR_NAME, conf, parameters));
    }

    /**
     * Builds MDStore service facade.
     * 
     * @param parameters set of parameters configuring {@link MDStoreFacade}, 
     * at least {@value AbstractEntityExporterProcess#MDSTORE_FACADE_FACTORY_CLASS} parameter is required
     */
    private MDStoreFacade buildMDStoreFacade(Map<String, String> parameters) {
        String serviceFactoryClassName = parameters.get(MDSTORE_FACADE_FACTORY_CLASS);
        Preconditions.checkArgument(StringUtils.isNotBlank(serviceFactoryClassName), 
                "unknown service facade factory, no '%s' parameter provided!", MDSTORE_FACADE_FACTORY_CLASS);
        try {
            Class<?> clazz = Class.forName(serviceFactoryClassName);
            Constructor<?> constructor = clazz.getConstructor();
            MDStoreFacadeFactory serviceFactory = (MDStoreFacadeFactory) constructor.newInstance();
            return serviceFactory.create(parameters);
        } catch (Exception e) {
            throw new RuntimeException("exception occurred while instantiating service by facade factory: " + 
                    MDSTORE_FACADE_FACTORY_CLASS, e);
        }
    }

    /**
     * Creates action factory transforming MDStore records into actions.
     * 
     */
    private ActionFactory buildActionFactory() {
        Map<String, Resource> xslts = new HashMap<String, Resource>();
        xslts.put(entityXSLTName, new ClassPathResource(entityXSLTLocation));
        ActionFactory actionFactory = new ActionFactory();
        actionFactory.setXslts(xslts);
        return actionFactory;
    }

    /**
     * Converts entity identifier to MDStore internal entity id by removing result entity prefix being part of InfoSpace model.
     * 
     * @param id source entity identifier to be processed
     * @return MDStore compliant entity identifier
     */
    private final String convertToMDStoreEntityId(String id) {
        if (id != null && id.startsWith(HBaseConstants.ROW_PREFIX_RESULT)) {
            return id.substring(HBaseConstants.ROW_PREFIX_RESULT.length());
        } else {
            return id;
        }
    }

    // ------------------------ INNER CLASS ----------------------------------
    
    public class MDStoreIdWithEntityId {
        
        private final String mdStoreId;
        private final String entityId;

        // ------------------------ CONSTRUCTORS ----------------------------------
        
        public MDStoreIdWithEntityId(String mdStoreId, String entityId) {
            this.mdStoreId = mdStoreId;
            this.entityId = entityId;
        }

        // ------------------------ GETTERS ----------------------------------        
        
        public String getMdStoreId() {
            return mdStoreId;
        }

        public String getEntityId() {
            return entityId;
        }
    }

}
