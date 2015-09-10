package eu.dnetlib.iis.common;

/**
 * Byte array utility class.
 * @author mhorst
 *
 */
public class ByteArrayUtils {

	/**
	 * Does this byte array begin with match array content?
	 * @param source Byte array to examine
	 * @param match Byte array to locate in <code>source</code>
	 * @return true If the starting bytes are equal
	 */
	public static boolean startsWith(byte[] source, byte[] match) {
		return startsWith(source, 0, match);
	}

	/**
	 * Does this byte array begin with match array content?
	 * @param source Byte array to examine
	 * @param offset An offset into the <code>source</code> array
	 * @param match Byte array to locate in <code>source</code>
	 * @return true If the starting bytes are equal
	 */
	public static boolean startsWith(byte[] source, int offset, byte[] match) {
		if (match.length > (source.length - offset)) {
			return false;
		}
		for (int i = 0; i < match.length; i++) {
			if (source[offset + i] != match[i]) {
				return false;
			}
		}
		return true;
	}
	
}
