package eu.dnetlib.iis.workflows.importer.mapred.helper;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Base64;

/**
 * Scan string generator based on:
 * http://blog.ozbuyucusu.com/2011/07/21/using-hbase-tablemapper-via-oozie-workflow/
 * @author mhorst
 *
 */
public class ScanStringGenerator {

	public static final String DEFAULT_ENCODING = "utf-8";
	public static final char DEFAULT_CF_CSV_SEPARATOR = ',';
	
    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException {
//    	preparing options
    	Options options = new Options();
    	options.addOption("c", "cacheSize", true, "scanner caching size: " +
    			"number of rows for caching that will be passed to scanners");
    	options.addOption("s", "startWith", true, "element to start iteration with");
    	options.addOption("e", "endWith", true, "element to end iteration with");
    	options.addOption("r", "rowPrefix", true, "row prefix");
    	options.addOption("f", "columnFamilies", true, "CSV containing comma separated " +
    			"supported column families");
    	options.addOption("x", "encoding", true, "encoding to be used for building byte[] data from parameters," +
    			"set to " + DEFAULT_ENCODING + " by default");
    	
//    	parsing parameters
    	CommandLineParser parser = new GnuParser();
        CommandLine cmdLine = parser.parse(options, args);
        
        String encoding = cmdLine.hasOption("x")?cmdLine.getOptionValue("x"):DEFAULT_ENCODING;
        
        Scan scan = new Scan();
        if (cmdLine.hasOption("c")) {
        	scan.setCaching(Integer.valueOf(cmdLine.getOptionValue("c")));
        }
        if (cmdLine.hasOption("s")) {
        	scan.setStartRow(cmdLine.getOptionValue("s").getBytes(encoding));
        }
        if (cmdLine.hasOption("e")) {
        	scan.setStopRow(cmdLine.getOptionValue("e").getBytes(encoding));
        }
        if (cmdLine.hasOption("r")) {
//        	supporting multiple prefixes 
        	String[] rowPrefixCSV = StringUtils.split(cmdLine.getOptionValue("r"), 
        			DEFAULT_CF_CSV_SEPARATOR);
        	if (rowPrefixCSV!=null) {
        		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        		for (String currentRowPrefix : rowPrefixCSV) {
        			filterList.addFilter(new PrefixFilter(
        					copyArrayWhenNotNull(currentRowPrefix.trim().getBytes(encoding))));
        		}
        		scan.setFilter(filterList);
        	}
        }
        
        if (cmdLine.hasOption("f")) {
        	String[] cfCSV = StringUtils.split(cmdLine.getOptionValue("f"), 
        			DEFAULT_CF_CSV_SEPARATOR);
        	if (cfCSV!=null) {
        		for (String currentCf : cfCSV) {
        			scan.addFamily(
    						copyArrayWhenNotNull(currentCf.trim().getBytes(encoding)));
        		}
        	}
        }

        File file = new File(System.getProperty("oozie.action.output.properties"));
        Properties props = new Properties();
        props.setProperty("scan", convertScanToString(scan));
        OutputStream os = new FileOutputStream(file);
        try {
        	props.store(os, "");	
        } finally {
        	os.close();	
        }
    }

    private static String convertScanToString(Scan scan) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        scan.write(dos);
        return Base64.encodeBytes(out.toByteArray());
    }
    
    /**
	  * Copies array or returns null when source is null.
	 * @param source
	 * @return copied array
	 */
	final public static byte[] copyArrayWhenNotNull(byte[] source) {
		if (source!=null) {
			return Arrays.copyOf(source, source.length);
		} else {
			return null;
		}
	}
}
