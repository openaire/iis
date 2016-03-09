package eu.dnetlib.iis.wf.metadataextraction;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.InvalidParameterException;
import org.jdom.Element;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;
import pl.edu.icm.cermine.ContentExtractor;

/**
 * Metadata extractor main class executing extraction 
 * for all files provided as arguments.
 * @author mhorst
 *
 */
public class MetadataExtractorMain {

	public static void main(String[] args) throws Exception {
		if (args.length>0) {
			for (String fileLoc : args) {
				ContentExtractor extractor = new ContentExtractor();
				InputStream inputStream = new FileInputStream(new File(fileLoc));
				try {
                    extractor.setPDF(inputStream);
					Element resultElem = extractor.getNLMContent();
					XMLOutputter outputter = new XMLOutputter(Format.getPrettyFormat());
					System.out.println(outputter.outputString(resultElem));
					System.out.println();
				} finally {
					inputStream.close();
				}
			}
		} else {
			throw new InvalidParameterException("no pdf file path provided");
		}
	}

}
