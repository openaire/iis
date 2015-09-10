package eu.dnetlib.iis.mainworkflows.importer.acm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import pl.edu.icm.cermine.bibref.CRFBibReferenceParser;

/**
 * {@link CRFBibReferenceParser} performance runner.
 * Two optional parameters can be provided
 * args[0] - directory holding text files with references (one per line)
 * args[1] - tested package size, 1000 by default
 * @author mhorst
 *
 */
public class BibrefParserPerformanceRunner {

	static final int defaultPackageSize = 1000;
	
	public static void main(String[] args) throws Exception {
		CRFBibReferenceParser bibrefParser = CRFBibReferenceParser.getInstance();
		String dirLocation = "/home/azio/Downloads/mh_acm_references";
		int packageSize = defaultPackageSize;
		
		if (args.length>0) {
			dirLocation = args[0];
			if (args.length>1) {
				packageSize = Integer.valueOf(args[1]);
			}
		}
		
		System.out.println("setting source dir to: " + dirLocation);
		File sourceDir = new File(dirLocation);
		
		File[] files = sourceDir.listFiles();
		
		int processedCount = 0;
		long currentIntermediateTime = System.currentTimeMillis();
		
		for (File currentFile: files) {
			System.out.println("processing file: " + currentFile.getName());
			BufferedReader reader = new BufferedReader(new FileReader(currentFile));
			try {
				String line = null;
				while((line = reader.readLine())!=null) {
					bibrefParser.parseBibReference(line);
					processedCount++;
					if (processedCount%packageSize==0) {
						System.out.println("processed " + processedCount + " references, "
								+ "time taken to process " + packageSize + " entries: " +
								((System.currentTimeMillis()-currentIntermediateTime)/1000) + " seconds");
						currentIntermediateTime = System.currentTimeMillis();
					}
				}
			} finally {
				reader.close();
			}
		}
	}
}
