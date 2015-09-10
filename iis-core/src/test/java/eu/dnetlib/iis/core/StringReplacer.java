package eu.dnetlib.iis.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Replace strings conforming to given regexes "on the flight", i.e. while copying
 * file from one place to another.
 *
 * This is used in order to change the Oozie's "workflow.xml" so that it 
 * conforms to the requirements of the MiniOozie testing engine.
 * 
 * @author Mateusz Kobos
 *
 */
public class StringReplacer {
	private static final List<StringReplacement> replacements = getReplacements();
	
	private static List<StringReplacement> getReplacements(){
		List<StringReplacement> list = new ArrayList<StringReplacement>();

		/** We have to use a special wrapper for Java workflow nodes in order
		 * to make them use the special mock HDFS used during the tests.
		 */
		list.add(new StringReplacement(
				"ProcessWrapper", 
				"ProcessWrapperForTests"));

		/** The paths to Avro schemas stored in HDFS are understood differently 
		 * when running on real cluster and during the tests. When running
		 * during the tests, a file system prefix "hdfs://" is absent, which
		 * confuses the "avro-json" library that allows for reading and writing
		 * Avro files in Hadoop Streaming workflow node. See the code of
		 * {@code com.cloudera.science.avro.common.SchemaLoader.loadFromUrl()}
		 * method for details where this confusion comes from. 
		 * 
		 * Here, we're adding a prefix which is the address of the name
		 * node which is something that "avro-json" library expects.
		 */
		list.add(new StringReplacement(
				"<value>(\\s*\\$\\{wf:appPath\\(\\)\\}.*\\.avsc\\s*)</value>",
				"<value>\\${nameNode}$1</value>"));

		return list;
	}
	
	public void replace(File in, File out) throws IOException {
		BufferedReader reader = null;
		PrintWriter writer = null;
		try {
			reader = new BufferedReader(new FileReader(in));
			writer = new PrintWriter(new FileWriter(out));
			String line = null;
			while ((line = reader.readLine()) != null){
				String newLine = line;
				for(StringReplacement replacement: replacements){
					newLine = replacement.replaceAll(newLine);
				}
				writer.println(newLine);
			}
		} finally {
			if(reader != null){
				reader.close();
			}
			if (writer != null){
				writer.close();
			}
		}
	}
}

class StringReplacement{
	private String regex;
	private String replacement;
	
	public StringReplacement(String regex, String replacement) {
		this.regex = regex;
		this.replacement = replacement;
	}
	
	public String replaceAll(String str){
		return str.replaceAll(regex, replacement);
	}
}
