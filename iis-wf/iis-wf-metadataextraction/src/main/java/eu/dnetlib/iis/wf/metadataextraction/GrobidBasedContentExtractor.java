package eu.dnetlib.iis.wf.metadataextraction;

import java.io.File;
import java.util.Collections;

import org.grobid.core.*;
import org.grobid.core.data.*;
import org.grobid.core.factory.*;
import org.grobid.core.main.GrobidHomeFinder;
import org.grobid.core.main.LibraryLoader;
import org.grobid.core.utilities.*;
import org.grobid.core.engines.Engine;
import org.grobid.core.engines.config.GrobidAnalysisConfig;

/**
 * Simple content extractor relying on Grobid library.
 * 
 * @author mhorst
 */
public class GrobidBasedContentExtractor {

    private static String grobidHome = "/home/mhorst/grobid-home";
    
    public static void initializeGrobid() {
        
        GrobidHomeFinder grobidHomeFinder = new GrobidHomeFinder(Collections.singletonList(grobidHome));
        GrobidProperties.getInstance(grobidHomeFinder);

        // Load native libraries required for Grobid
        LibraryLoader.load();
    }
    
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String fileLocation = "/home/mhorst/Downloads/HPM-35-1009.pdf";
        
        extractMeta(fileLocation);
        extractText(fileLocation);
    }
    
    private static void extractMeta(String fileLocation) {
        Engine engine = GrobidFactory.getInstance().createEngine();
        // 0 - no consolidation
        int consolidate = 0;
        try {
            BiblioItem result = new BiblioItem();
            engine.processHeader(fileLocation, consolidate, result);
            System.out.println("Title: " + result.getTitle());
            System.out.println("Authors: " + result.getAuthors());
            System.out.println("Abstract: " + result.getAbstract());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static void extractText(String fileLocation) {
        Engine engine = GrobidFactory.getInstance().createEngine();
        try {
            GrobidAnalysisConfig config = GrobidAnalysisConfig.builder().consolidateHeader(0).build();
            File file = new File(fileLocation);
            String fullText = engine.fullTextToTEI(file, config);
            System.out.println("Extracted Text: " + fullText);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
