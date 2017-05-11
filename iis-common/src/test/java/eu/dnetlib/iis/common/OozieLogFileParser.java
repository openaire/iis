package eu.dnetlib.iis.common;

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

    //------------------------ CONSTRUCTORS -------------------
    
    private OozieLogFileParser() {}
    
	//------------------------ LOGIC --------------------------
	
	/**
	 * Returns oozie job id based on log file
	 */
	public static String readJobIdFromLogFile(File runOozieJobLogFile) {
		
		String jobId;
		try {
			jobId = FileUtils.readFileToString(runOozieJobLogFile, InfoSpaceConstants.ENCODING_UTF8);
		} catch (IOException e) {
			throw new RuntimeException("Unable to read run oozie job log file", e);
		}
		Pattern pattern = Pattern.compile("^job: (\\S*)$", Pattern.MULTILINE);
		Matcher matcher = pattern.matcher(jobId);
		if (matcher.find()) {
			jobId = matcher.group(1);
		} else {
			throw new RuntimeException("Unable to find jobId in oozie job log file. Check if oozie job has started");
		}
		
		return jobId;
	}
}
