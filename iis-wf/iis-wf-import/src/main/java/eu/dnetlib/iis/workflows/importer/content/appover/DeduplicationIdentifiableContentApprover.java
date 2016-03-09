package eu.dnetlib.iis.workflows.importer.content.appover;

import java.util.HashSet;
import java.util.Set;

/**
 * Content deduplicator by identifier.
 * @author mhorst
 *
 */
public class DeduplicationIdentifiableContentApprover implements
		IdentifiableContentApprover {

	private final Set<String> alreadyWritten = new HashSet<String>();
	
	@Override
	public boolean approve(String id, byte[] content) {
		if (alreadyWritten.contains(id)) {
			return false;
		} else {
			alreadyWritten.add(id);
			return true;
		}
	}

}
