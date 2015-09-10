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
		xstream.processAnnotations(BlobCitationEntry.class);
		xstream.alias("citations", SortedSet.class);
	}

	@SuppressWarnings("unchecked")
	@Override
	public SortedSet<BlobCitationEntry> deserialize(String source) throws UnsupportedOperationException {
		return (SortedSet<BlobCitationEntry>) xstream.fromXML(source);
	}
}
