package eu.dnetlib.iis.workflows.export.actionmanager.entity.dataset;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import eu.dnetlib.actionmanager.ActionManagerConstants;
import eu.dnetlib.actionmanager.actions.ActionFactory;
import eu.dnetlib.actionmanager.actions.XsltInfoPackageAction;
import eu.dnetlib.actionmanager.common.Agent;
import eu.dnetlib.actionmanager.common.Agent.AGENT_TYPE;
import eu.dnetlib.actionmanager.common.Operation;
import eu.dnetlib.actionmanager.common.Provenance;
import eu.dnetlib.data.mdstore.MDStoreService;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.enabling.tools.JaxwsServiceResolverImpl;

public class DatasetExporterProcessManualTest {

	public static void main(String[] args) throws Exception {
		String mdStoretLocation = "http://node0.t.openaire.research-infrastructures.eu:8080/is/services/mdStore";
		String dataciteMDStoreId = "3bdae693-de10-4aa6-8741-f6e410ddd007_TURTdG9yZURTUmVzb3VyY2VzL01EU3RvcmVEU1Jlc291cmNlVHlwZQ==";
		
		W3CEndpointReferenceBuilder eprBuilder = new W3CEndpointReferenceBuilder();
		eprBuilder.address(mdStoretLocation);
		eprBuilder.build();
		MDStoreService mdStore = new JaxwsServiceResolverImpl().getService(
				MDStoreService.class, eprBuilder.build());
		String mdStoreRecord = mdStore.deliverRecord(
				dataciteMDStoreId,
//				SOAPFaultException
//				"datacite____::0087dac37c50cf3aa6a2a5f2848917ff");
//				was SOAPFaultException but now is OK
//				"datacite____::a5fd123bab119524f47770c805dbd0f9");
//				NPEx
//				"datacite____::3bb9550fdd4d4bca48778d6da0615ab9");
//				OK
//				"datacite____::00000e595b5f5fc976b39137b7dbf62e");
//				new NPEx from 10.03.2014
				"datacite____::65472a97751972d7c82571ea97e8f989");
		System.out.println("MDStore record contents:");
		System.out.println(mdStoreRecord);
		
		Map<String,Resource> xslts = new HashMap<String, Resource>();
		String dataciteXSLT = "datacite2actions";
		xslts.put(dataciteXSLT, new ClassPathResource(
				"eu/dnetlib/actionmanager/xslt/datacite2insertActions.xslt"));
		ActionFactory actionFactory = new ActionFactory();
		actionFactory.setXslts(xslts);
		
		XsltInfoPackageAction action = actionFactory.generateInfoPackageAction(
				dataciteXSLT, "actionSetId", 
				new Agent("iis", "information inference service", AGENT_TYPE.service),
				Operation.INSERT, mdStoreRecord, 
				Provenance.sysimport_mining_repository, "datacite____", "0.9");
		List<Put> puts = action.asPutOperations();
		System.out.println("generated actions:");
		for (Put put : puts) {
			System.out.println(put.toJSON());
			List<KeyValue> results = put.get(
					ActionManagerConstants.TARGET_COLFAMILY, 
					ActionManagerConstants.TARGET_OAF_COL);
			if (results!=null) {
				for (KeyValue currentRes : results) {
//					System.out.println("got content: " + new String(currentRes.getValue()));
					Oaf.Builder oafBuilder = Oaf.newBuilder();
					oafBuilder.mergeFrom(currentRes.getValue());
					Oaf oaf = oafBuilder.build();
					System.out.println("got content: " + oaf.toString());
				}
			}
		}
	}
	
}
