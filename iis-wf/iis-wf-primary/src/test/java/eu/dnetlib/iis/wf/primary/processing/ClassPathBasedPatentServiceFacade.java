package eu.dnetlib.iis.wf.primary.processing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;

import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;
import eu.dnetlib.iis.wf.referenceextraction.patent.PatentServiceException;
import eu.dnetlib.iis.wf.referenceextraction.patent.PatentServiceFacade;

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

    private static final String classPathRoot = "/eu/dnetlib/iis/wf/primary/processing/data/patent/mock_facade_storage/";

    @Override
    public String getPatentMetadata(ImportedPatent patent) throws NoSuchElementException, PatentServiceException {
        try {
            return getContent(classPathRoot + generateFileName(patent));
        } catch (Exception e) {
            throw new PatentServiceException(e);
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
        return IOUtils.toString(ClassPathBasedPatentServiceFacade.class.getResourceAsStream(location),
                StandardCharsets.UTF_8);
    }

}

