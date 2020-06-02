package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;

import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;

/**
 * Simple mock retrieving XML concents as files from classpath. Relies on
 * publn_auth, publn_nr, publn_kind fields defined in {@link ImportedPatent}
 * while generating filename:
 * 
 * publn_auth + '.' + publn_nr + '.' + publn_kind + ".xml"
 * 
 * @author mhorst
 *
 */
public class ClassPathBasedPatentServiceFacade implements PatentServiceFacade {

    private static final long serialVersionUID = 1L;

    private static final String classPathRoot = "/eu/dnetlib/iis/wf/referenceextraction/patent/data/mock_facade_storage/";

    @Override
    public String getPatentMetadata(ImportedPatent patent) throws NoSuchElementException, Exception {
        try {
            return getContent(classPathRoot + generateFileName(patent));
        } catch (IOException e) {
            return "";
        }
    }

    private static String generateFileName(ImportedPatent patent) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(patent.getPublnAuth());
        strBuilder.append('.');
        strBuilder.append(patent.getPublnNr());
        strBuilder.append('.');
        strBuilder.append(patent.getPublnKind());
        strBuilder.append(".xml");
        return strBuilder.toString();
    }

    private static String getContent(String location) throws IOException {
        return IOUtils.toString(ClassPathBasedPatentServiceFacade.class.getResourceAsStream(location), "utf8");
    }

}
