package eu.dnetlib.iis.common;

import eu.dnetlib.iis.common.java.io.JsonUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Utility class for tests dealing with IO
 * 
 * @author Mateusz Kobos
 * 
 */
public class TestsIOUtils {

	private final static int filesCompareBufferSize = 1024;
	private final static int dumpRecordsCountlimit = 100;
	
	private TestsIOUtils() {}
	
	/**
	 * {@code expected} and {@code actual} are equal if they have the same
	 * elements, possibly in different order.
	 */
	public static <T> void assertEqualSets(List<T> expected, Iterable<T> actual) {
		assertEqualSets(expected, actual.iterator());
	}

	/**
	 * {@code expected} and {@code actual} are equal if they have the same
	 * elements, possibly in different order.
	 */
	public static <T> void assertEqualSets(List<T> expected, Iterator<T> actual) {
		List<T> actualList = new ArrayList<T>();
		while(actual.hasNext()) {
			actualList.add(actual.next());
		}
		assertEqualSets(expected, actualList);
	}
	
	/**
	 * {@code expected} and {@code actual} are equal if they have the same
	 * elements, possibly in different order.
	 */
	public static <T> void assertEqualSets(
			List<T> expected, List<T> actual) {
		assertEqualSets(expected, actual, false);
	}
	
	/**
	 * {@code expected} and {@code actual} are equal if they have the same
	 * elements, possibly in different order.
	 * @param toPrettyJson if the sets are not equal, an error message with
	 * elements of both lists is printed. If this parameter is set to 
	 * {@code true}, the string representation of each element is treated as 
	 * JSON and printed in a pretty way.
	 */
	public static <T> void assertEqualSets(
			List<T> expected, List<T> actual, boolean toPrettyJson) {
		assertEquals(expected.size(), actual.size(), String.format("The number of the " +
						"expected elements (%s) is not equal to the number " +
						"of the actual elements (%s).\n\n%s",
				expected.size(), actual.size(),
				elementsToString(expected, actual, toPrettyJson)));
		boolean[] actualMatched = new boolean[actual.size()];
		for(int i = 0; i < expected.size(); i++) {
			boolean iMatched = false;
			for(int j = 0; j < actual.size(); j++) {
				if (actualMatched[j]) {
					continue;
				}
				if (expected.get(i).equals(actual.get(j))) {
					actualMatched[j] = true;
					iMatched = true;
					break;
				}
			}
			
			String additionalElementRawStr = expected.get(i).toString();
			String additionalElementStr = null;
			if(!toPrettyJson){
				additionalElementStr = String.format(
					" '%s' ", additionalElementRawStr);
			} else {
				additionalElementStr = String.format(
					"\n%s\n", JsonUtils.toPrettyJSON(additionalElementRawStr));
			}
			
			assertTrue(iMatched, String.format("The element%sthat is " +
				"present among expected elements was not found among the " +
				"actual elements.\n\n%s", additionalElementStr,
				elementsToString(expected, actual, toPrettyJson)));
		}
	}
	
	private static <T> String elementsToString(
			List<T> expected, List<T> actual, boolean toPrettyJson){
		StringBuilder builder = new StringBuilder();
		builder.append("Please find a dump of expected and actual records below.\n\n");
		builder.append(singleListToString(expected, "\nExpected", toPrettyJson));
		builder.append(singleListToString(actual, "\n\nActual", toPrettyJson));
		return builder.toString();
	}
	
	private static <T> String singleListToString(
			List<T> elems, String name, boolean toPrettyJson){
		StringBuilder builder = new StringBuilder();
		builder.append(name + " records:\n");
		for(int i = 0; i < dumpRecordsCountlimit; i++){
            if(i == elems.size()){
				break;
			}
            String elemString = elems.get(i).toString();
            if(toPrettyJson){
            	elemString = JsonUtils.toPrettyJSON(elemString);
            }
			builder.append(elemString+"\n");
		}
		if(elems.size() > dumpRecordsCountlimit){
			builder.append("... (more records available but not shown "+
					"due to print limit of "+dumpRecordsCountlimit+" records)\n");
		}
		return builder.toString();
	}
	
	/**
	 * Check whether contents of two files are equal. A utility function.
	 * @param resourcePath
	 * @param otherFile
	 */
	public static void assertContentsEqual(String resourcePath, File otherFile) 
			throws FileNotFoundException, IOException{
		assertEqual(ClassPathResourceProvider.getResourceInputStream(resourcePath), new FileInputStream(otherFile));
	}
	
    /**
     * Checks whether the passed input streams are equal. The input streams are considered to be equal if their content is
     * the same utf-8 text (windows/unix new line differences do not matter - they are treated as if they were the same).
    */
    public static void assertUtf8TextContentsEqual(InputStream in0, InputStream in1) {
	    
        String line1 = null;
        String line2 = null;
        
        try (BufferedReader br1 = new BufferedReader(new InputStreamReader(in0, "UTF-8"))) {
            try (BufferedReader br2 = new BufferedReader(new InputStreamReader(in1, "UTF-8"))) {
                while ((line1 = br1.readLine()) != null & (line2 = br2.readLine()) != null) {
                    assertEquals(line1, line2);
                }
                if (line1 != null || line2 != null) {
                    fail();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
	
	
	/**
	 * Assume that the contents of two input streams is equal.
	 * 
	 * The streams are closed after the comparison is made.
	 */
	public static void assertEqual(InputStream in0, InputStream in1){
		try {
			assertTrue(equal(in0, in1));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Check if contents of two input streams is equal.
	 * 
	 * The streams are closed after the comparison is made.
	 */
	private static boolean equal(InputStream in0, InputStream in1) 
			throws IOException {
		try {
			byte[] buffer0 = new byte[filesCompareBufferSize];
			byte[] buffer1 = new byte[filesCompareBufferSize];
			int bytesRead0 = 0;
			int bytesRead1 = 0;
			while (bytesRead0 > -1) {
				bytesRead0 = in0.read(buffer0);
				bytesRead1 = in1.read(buffer1);
				if (bytesRead0 != bytesRead1)
					return false;
				if (!Arrays.equals(buffer0, buffer1))
					return false;
			}
			return true;
		} finally {
			if (in0 != null)
				in0.close();
			if (in1 != null)
				in1.close();
		}
	}

}
