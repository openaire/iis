package eu.dnetlib.iis.workflows.importer.converter;


import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;

import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.iis.workflows.importer.OafHelper;
import eu.dnetlib.iis.workflows.importer.input.approver.ResultApprover;
import eu.dnetlib.iis.common.schemas.IdentifierMapping;

/**
 * {@link IdentifierMapping} converter.
 * @author mhorst
 *
 */
public class DeduplicationMappingConverter extends AbstractAvroConverter<IdentifierMapping[]> {

	/**
	 * dedupRel relation column family.
	 */
	private final byte[] resultResultDedupMergesColumnFamilyBytes;
	
	/**
	 * Default constructor.
	 * @param encoding
	 * @param resultApprover
	 * @param resultResultDedupMergesColumnFamilyBytes
	 */
	public DeduplicationMappingConverter(String encoding,
			ResultApprover resultApprover,
			byte[] resultResultDedupMergesColumnFamilyBytes) {
		super(encoding, resultApprover);
		this.resultResultDedupMergesColumnFamilyBytes = OafHelper.copyArrayWhenNotNull(
				resultResultDedupMergesColumnFamilyBytes);
	}

	@Override
	public IdentifierMapping[] buildObject(Result hbaseResult,
			Oaf resolvedOafObject) throws InvalidProtocolBufferException {
		NavigableMap<byte[],byte[]> dedupRelations = hbaseResult.getFamilyMap(
				this.resultResultDedupMergesColumnFamilyBytes);
		if (dedupRelations!=null && dedupRelations.size()>0) {
			List<IdentifierMapping> results = new ArrayList<IdentifierMapping>(
					dedupRelations.size());
			for (byte[] resultResultBytes : dedupRelations.values()) {
				Oaf resResOAF = OafHelper.buildOaf(resultResultBytes);
				OafRel resResRel = resResOAF.getRel();
				if (resultApprover!=null?
						resultApprover.approveBeforeBuilding(resResOAF):
							true) {
					IdentifierMapping.Builder builder = IdentifierMapping.newBuilder();
					builder.setNewId(resolvedOafObject.getEntity().getId());
					builder.setOriginalId(resResRel.getTarget());
					results.add(builder.build());
				}
			}
			return results.toArray(new IdentifierMapping[results.size()]);
		}
//		fallback
		return null;
	}

}
