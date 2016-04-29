package eu.dnetlib.iis.wf.importer.converter;


import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;

import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.iis.importer.schemas.DocumentToProject;
import eu.dnetlib.iis.wf.importer.OafHelper;
import eu.dnetlib.iis.wf.importer.input.approver.ResultApprover;

/**
 * {@link DocumentToProject} converter.
 * @author mhorst
 *
 */
public class DocumentToProjectConverter extends AbstractAvroConverter<DocumentToProject[]> {

	/**
	 * Result-project relation column family.
	 */
	private final byte[] resultProjectOutcomeIsProducedByColumnFamilyBytes;
	
	/**
	 * Default constructor.
	 * @param resultApprover
	 * @param resultProjectOutcomeIsProducedByColumnFamilyBytes
	 */
	public DocumentToProjectConverter(
			ResultApprover resultApprover,
			byte[] resultProjectOutcomeIsProducedByColumnFamilyBytes) {
		super( resultApprover);
		this.resultProjectOutcomeIsProducedByColumnFamilyBytes = OafHelper.copyArrayWhenNotNull(
				resultProjectOutcomeIsProducedByColumnFamilyBytes);
	}

	@Override
	public DocumentToProject[] buildObject(Result hbaseResult,
			Oaf resolvedOafObject) throws InvalidProtocolBufferException {
		NavigableMap<byte[],byte[]> resProjRelations = hbaseResult.getFamilyMap(
				resultProjectOutcomeIsProducedByColumnFamilyBytes);
		if (resProjRelations!=null && resProjRelations.size()>0) {
			List<DocumentToProject> results = new ArrayList<DocumentToProject>(
					resProjRelations.size());
			for (byte[] resProjBytes : resProjRelations.values()) {
				Oaf resultProjOAF = OafHelper.buildOaf(resProjBytes);
				OafRel resultProjRel = resultProjOAF.getRel();
				if (resultApprover!=null?
						resultApprover.approveBeforeBuilding(resultProjOAF):
							true) {
					DocumentToProject.Builder builder = DocumentToProject.newBuilder();
					builder.setDocumentId(resultProjRel.getSource());
					builder.setProjectId(resultProjRel.getTarget());
					results.add(builder.build());
				}
			}
			return results.toArray(new DocumentToProject[results.size()]);
		}
//		fallback
		return null;
	}

}
