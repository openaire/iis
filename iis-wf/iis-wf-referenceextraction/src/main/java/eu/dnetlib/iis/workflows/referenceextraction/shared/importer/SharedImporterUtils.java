package eu.dnetlib.iis.workflows.referenceextraction.shared.importer;


/**
 * Shared importer utils.
 * @author mhorst
 *
 */
public class SharedImporterUtils {
	
	/**
	 * Method checking whether given line should be skipped.
	 * @param line
	 * @param skipPrefixes
	 * @return true when should be skipped, false otherwise
	 */
	public static boolean skipLine(String line, String... skipPrefixes) {
		if (line.isEmpty()) {
			return true;
		} else {
			if (skipPrefixes!=null && skipPrefixes.length>0) {
				for (String skipPrefix : skipPrefixes) {
					if (line.startsWith(skipPrefix)) {
						return true;
					}
				}
			}
		}
//		fallback
		return false;
	}
	
}
