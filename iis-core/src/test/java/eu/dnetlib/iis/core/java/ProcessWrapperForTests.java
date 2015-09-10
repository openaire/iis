package eu.dnetlib.iis.core.java;

import org.apache.hadoop.conf.Configuration;

import eu.dnetlib.iis.core.OozieTestsIOUtils;
import eu.dnetlib.iis.core.java.ProcessWrapper;

/**
 * A version of the {@link ProcessWrapper} which uses the Configuration
 * object created by the Oozie unit test framework. This is used in the
 * XML workflow definition in the tests instead of {@link ProcessWrapper}. 
 * It makes referencing the underlying file system by the java Process run
 * during the tests possible.
 * @author Mateusz Kobos
 * 
 */
public class ProcessWrapperForTests extends ProcessWrapper {
	public Configuration getConfiguration() throws Exception {
		return OozieTestsIOUtils.loadConfiguration();
	}
	
	public static void main(String[] args) throws Exception {
		ProcessWrapper wrapper = new ProcessWrapperForTests();
		wrapper.run(args);
	}
}
