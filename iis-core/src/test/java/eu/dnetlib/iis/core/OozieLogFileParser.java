package eu.dnetlib.iis.core;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

/**
 * Parser of oozie log files
 * 
 * @author madryk
 *
 */
class OozieLogFileParser {

	//------------------------ LOGIC --------------------------
	
	/**
	 * Returns oozie job id based on log file
	 */
	public static String readJobIdFromLogFile(File runOozieJobLogFile) {
		
		String jobId;
		try {
			jobId = FileUtils.readFileToString(runOozieJobLogFile);
		} catch (IOException e) {
			throw new RuntimeException("Unable to read run oozie job log file", e);
		}
		Pattern pattern = Pattern.compile("^job: (\\S*)$", Pattern.MULTILINE);
		Matcher matcher = pattern.matcher(jobId);
		matcher.find();
		jobId = matcher.group(1);
		
		return jobId;
	}
}
