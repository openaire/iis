package eu.dnetlib.iis.common.model.extrainfo.converter;

import java.util.SortedSet;

import eu.dnetlib.iis.common.model.extrainfo.citations.BlobCitationEntry;


/**
 * {@link BlobCitationEntry} based avro to xml converter.
 * @author mhorst
 *
 */
public class CitationsExtraInfoConverter extends AbstractExtraInfoConverter<SortedSet<BlobCitationEntry>> {

	public CitationsExtraInfoConverter() {
	    super();
		getXstream().processAnnotations(BlobCitationEntry.class);
		getXstream().alias("citations", SortedSet.class);
	}

	@SuppressWarnings("unchecked")
	@Override
	public SortedSet<BlobCitationEntry> deserialize(String source) throws UnsupportedOperationException {
		return (SortedSet<BlobCitationEntry>) getXstream().fromXML(source);
	}
}
