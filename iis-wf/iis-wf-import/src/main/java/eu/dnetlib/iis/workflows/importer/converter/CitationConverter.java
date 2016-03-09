package eu.dnetlib.iis.workflows.importer.converter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import org.apache.hadoop.hbase.client.Result;

import eu.dnetlib.data.proto.FieldTypeProtos.ExtraInfo;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.iis.citationmatching.schemas.Citation;
import eu.dnetlib.iis.common.hbase.HBaseConstants;
import eu.dnetlib.iis.common.model.extrainfo.ExtraInfoConstants;
import eu.dnetlib.iis.common.model.extrainfo.citations.BlobCitationEntry;
import eu.dnetlib.iis.common.model.extrainfo.citations.TypedId;
import eu.dnetlib.iis.common.model.extrainfo.converter.CitationsExtraInfoConverter;
import eu.dnetlib.iis.workflows.importer.input.approver.ResultApprover;

/**
 * Converter producing {@link Citation} objects based on {@link ExtraInfo} element holding citation
 * XML representation.
 * @author mhorst
 *
 */
public class CitationConverter extends AbstractAvroConverter<Citation[]>{

	/**
	 * Citations XML blob converter.
	 */
	CitationsExtraInfoConverter citationExtraInfoConverter;
	
	public CitationConverter(String encoding, ResultApprover resultApprover) {
		super(encoding, resultApprover);
		citationExtraInfoConverter = new CitationsExtraInfoConverter();
	}

	@Override
	public Citation[] buildObject(Result hbaseResult, Oaf resolvedOafObject)
			throws IOException {
		if (resolvedOafObject!=null && resolvedOafObject.getEntity()!=null &&
				resolvedOafObject.getEntity().getExtraInfoList()!=null) {
			String sourceId = resolvedOafObject.getEntity().getId();
			List<Citation> results = new ArrayList<Citation>();
			for (ExtraInfo currentExtraInfo : resolvedOafObject.getEntity().getExtraInfoList()) {
				if (ExtraInfoConstants.TYPOLOGY_CITATIONS.equals(currentExtraInfo.getTypology()) &&
						currentExtraInfo.getValue()!=null) {
					SortedSet<BlobCitationEntry> citationSet = citationExtraInfoConverter.deserialize(
							currentExtraInfo.getValue());
					if (citationSet!=null && citationSet.size()>0) {
						for (BlobCitationEntry currentEntry : citationSet) {
							if (currentEntry.getIdentifiers()!=null) {
								for (TypedId currentTypedId : currentEntry.getIdentifiers()) {
									if (ExtraInfoConstants.CITATION_TYPE_OPENAIRE.equals(currentTypedId.getType())) {
										Citation.Builder citationBuilder = Citation.newBuilder();
										citationBuilder.setPosition(currentEntry.getPosition()>0?currentEntry.getPosition():-1);
										citationBuilder.setSourceDocumentId(sourceId);
//										adding "50|" prefix removed when exporting citations as BLOBs
										StringBuilder idBuilder = new StringBuilder();
										idBuilder.append(new String(HBaseConstants.ROW_PREFIX_RESULT, 
												HBaseConstants.STATIC_FIELDS_ENCODING_UTF8));
										idBuilder.append(currentTypedId.getValue());
										citationBuilder.setDestinationDocumentId(idBuilder.toString());
										citationBuilder.setConfidenceLevel(currentTypedId.getConfidenceLevel()
												/HBaseConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR);
										results.add(citationBuilder.build());
									}
								}	
							}
						}
					}
				}
			}
			return results.toArray(new Citation[results.size()]);
		}
//		fallback
		return null;
	}

}
