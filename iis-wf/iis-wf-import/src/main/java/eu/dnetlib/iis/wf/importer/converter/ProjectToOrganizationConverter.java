package eu.dnetlib.iis.wf.importer.converter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hbase.client.Result;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.iis.importer.schemas.ProjectToOrganization;
import eu.dnetlib.iis.wf.importer.OafHelper;
import eu.dnetlib.iis.wf.importer.input.approver.ResultApprover;

/**
 * {@link ProjectToOrganization} converter.
 * 
 * @author mhorst
 *
 */
public class ProjectToOrganizationConverter extends AbstractAvroConverter<Collection<ProjectToOrganization>> {

	/**
	 * Project-organization relation column family.
	 */
	private final byte[] relationColumnFamilyBytes;

	// ------------------------ CONSTRUCTORS --------------------------

	/**
	 * Default constructor.
	 * 
	 * @param resultApprover relation approver
	 * @param relationColumnFamilyBytes project-organization column family
	 */
	public ProjectToOrganizationConverter(ResultApprover resultApprover, byte[] relationColumnFamilyBytes) {
		super(resultApprover);
		Preconditions.checkNotNull(resultApprover);
		Preconditions.checkNotNull(relationColumnFamilyBytes);
		this.relationColumnFamilyBytes = OafHelper.copyArrayWhenNotNull(relationColumnFamilyBytes);
	}

	// ------------------------ LOGIC --------------------------

	/**
	 * Builds collection of {@link ProjectToOrganization} objects for given
	 * hbase input.
	 * 
	 * @param hbaseResult full hbase record
	 * @param resolvedOafObject resolved Oaf object
	 * @return collection of {@link ProjectToOrganization}
	 * @throws InvalidProtocolBufferException thrown when unable to deserialize object
	 */
	@Override
	public Collection<ProjectToOrganization> buildObject(Result hbaseResult, Oaf resolvedOafObject)
			throws InvalidProtocolBufferException {
		Preconditions.checkNotNull(hbaseResult);
		NavigableMap<byte[], byte[]> projOrgRelations = hbaseResult.getFamilyMap(relationColumnFamilyBytes);
		if (!MapUtils.isEmpty(projOrgRelations)) {
			List<ProjectToOrganization> projectOrganizationList = new ArrayList<ProjectToOrganization>(
					projOrgRelations.size());
			for (byte[] projOrgBytes : projOrgRelations.values()) {
				Oaf projOrgOaf = OafHelper.buildOaf(projOrgBytes);
				if (resultApprover.approveBeforeBuilding(projOrgOaf)) {
					OafRel projOrgRel = projOrgOaf.getRel();
					ProjectToOrganization.Builder builder = ProjectToOrganization.newBuilder();
					builder.setProjectId(projOrgRel.getSource());
					builder.setOrganizationId(projOrgRel.getTarget());
					projectOrganizationList.add(builder.build());
				}
			}
			return projectOrganizationList;
		} else {
			return Collections.emptyList();			
		}
	}

}
