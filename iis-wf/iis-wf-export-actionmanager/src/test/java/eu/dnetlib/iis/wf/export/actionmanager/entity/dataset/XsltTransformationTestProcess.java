package eu.dnetlib.iis.wf.export.actionmanager.entity.dataset;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.google.common.collect.Maps;

import eu.dnetlib.actionmanager.actions.ActionFactory;
import eu.dnetlib.actionmanager.actions.XsltInfoPackageAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.actionmanager.common.Agent.AGENT_TYPE;
import eu.dnetlib.actionmanager.common.Operation;
import eu.dnetlib.actionmanager.common.Provenance;
import eu.dnetlib.iis.common.WorkflowRuntimeParameters;
import eu.dnetlib.iis.common.java.PortBindings;
import eu.dnetlib.iis.common.java.Process;
import eu.dnetlib.iis.common.java.porttype.PortType;

/**
 * XSLT transformation test process.
 * @author mhorst
 *
 */
public class XsltTransformationTestProcess implements Process {

	public static final String PARAM_TRANSFORMER_FACTORY = "transformer_factory";

	private final Resource recordDataciteInputStream = new ClassPathResource(
			"/eu/dnetlib/iis/wf/export/actionmanager/xslt_test/data/recordDatacite.xml");

	private ActionFactory actionFactory;

	private String dataciteXSLT = "datacite2actions";

	private Agent agent = new Agent("agentId", "name", AGENT_TYPE.service);

	
	@Override
	public Map<String, PortType> getInputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, PortType> getOutputPorts() {
		return Collections.emptyMap();
	}

	@Override
	public void run(PortBindings portBindings, Configuration conf,
			Map<String, String> parameters) throws Exception {
		if (parameters!=null && !parameters.isEmpty()) {
			if (parameters.containsKey(PARAM_TRANSFORMER_FACTORY)) {
				String factory = parameters.get(PARAM_TRANSFORMER_FACTORY);
				if (factory!=null && !factory.isEmpty() && 
						!WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE.equals(factory)) {
					System.out.println("got " + PARAM_TRANSFORMER_FACTORY + " parameter, "
							+ "setting transformer property to: " + factory);
					System.setProperty(
							"javax.xml.transform.TransformerFactory", 
							factory);	
				}
			}
		}
		
		actionFactory = new ActionFactory();
		Map<String, Resource> xslts = Maps.newHashMap();
		xslts.put(dataciteXSLT, new ClassPathResource(
				"/eu/dnetlib/iis/wf/export/actionmanager/xslt_test/data/datacite2insertActions.xslt"));
		actionFactory.setXslts(xslts);

		String record = IOUtils.toString(recordDataciteInputStream.getInputStream());
		XsltInfoPackageAction action = actionFactory.generateInfoPackageAction(
				dataciteXSLT, "actionSetId_1", agent, 
				Operation.INSERT, record,
				Provenance.sysimport_mining_datasetarchive,
				"datacite____", "0.8");

		if (action!=null) {
			for (Put put : action.asPutOperations()) {
				if (put!=null) {
					System.out.println(put.toJSON());
				} else {
					throw new Exception("got null put!");
				}
			}
		} else {
			throw new Exception("generated action is null!");
		}
	}

	public static void main(String[] args) throws Exception {
		XsltTransformationTestProcess process = new XsltTransformationTestProcess();
		Map<String,String> parameters = new HashMap<String, String>();
		parameters.put(PARAM_TRANSFORMER_FACTORY, "com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl");
//		parameters.put(PARAM_TRANSFORMER_FACTORY, "org.apache.xalan.processor.TransformerFactoryImpl");
//		parameters.put(PARAM_TRANSFORMER_FACTORY, WorkflowRuntimeParameters.UNDEFINED_NONEMPTY_VALUE);
		process.run(null, null, parameters);
	}
}
