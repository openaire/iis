package eu.dnetlib.iis.wf.importer.converter;


import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;

import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.importer.OafHelper;
import eu.dnetlib.iis.wf.importer.input.approver.ResultApprover;

/**
 * {@link ProjectToOrganization} converter.
 * @author mhorst
 *
 */
public class ProjectToOrganizationConverter extends AbstractAvroConverter<ProjectToOrganization[]> {

	/**
	 * Project-organization relation column family.
	 */
	private final byte[] relationColumnFamilyBytes;
	
	/**
	 * Default constructor.
	 * @param encoding encoding to be used when converting byte[] to String
	 * @param resultApprover relation approver
	 * @param relationColumnFamilyBytes project-organization column family
	 */
	public ProjectToOrganizationConverter(String encoding,
			ResultApprover resultApprover,
			byte[] relationColumnFamilyBytes) {
		super(encoding, resultApprover);
		this.relationColumnFamilyBytes = OafHelper.copyArrayWhenNotNull(
				relationColumnFamilyBytes);
	}

	@Override
	public ProjectToOrganization[] buildObject(Result hbaseResult,
			Oaf resolvedOafObject) throws InvalidProtocolBufferException {
		NavigableMap<byte[],byte[]> projOrgRelations = hbaseResult.getFamilyMap(
				relationColumnFamilyBytes);
		if (projOrgRelations!=null && projOrgRelations.size()>0) {
			List<ProjectToOrganization> results = new ArrayList<ProjectToOrganization>(
					projOrgRelations.size());
			for (byte[] projOrgBytes : projOrgRelations.values()) {
				Oaf projOrgOAF = OafHelper.buildOaf(projOrgBytes);
				OafRel projOrgRel = projOrgOAF.getRel();
				if (resultApprover!=null?
						resultApprover.approveBeforeBuilding(projOrgOAF):
							true) {
					ProjectToOrganization.Builder builder = ProjectToOrganization.newBuilder();
					builder.setProjectId(projOrgRel.getSource());
					builder.setOrganizationId(projOrgRel.getTarget());
					results.add(builder.build());
				}
			}
			return results.toArray(new ProjectToOrganization[results.size()]);
		}
//		fallback
		return null;
	}

}
