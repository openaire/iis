package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.util.NoSuchElementException;

import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;

/**
 * Simple mock which provides concatenation of all {@link ImportedPatent} fields as generated meta.
 * 
 * @author mhorst
 *
 */
public class PatentFacadeMock implements PatentServiceFacade {

    private static final long serialVersionUID = 1L;

    @Override
    public String getPatentMetadata(ImportedPatent patent) throws NoSuchElementException, Exception {
        return generateMeta(patent);
    }

    protected static String generateMeta(ImportedPatent patent) {
        if ("non-existing".equals(patent.getPublnNr())) {
            throw new NoSuchElementException("unable to find element");
        }
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(patent.getApplnAuth());
        strBuilder.append('-');
        strBuilder.append(patent.getApplnNr());
        strBuilder.append('-');
        strBuilder.append(patent.getPublnAuth());
        strBuilder.append('-');
        strBuilder.append(patent.getPublnNr());
        strBuilder.append('-');
        strBuilder.append(patent.getPublnKind());
        return strBuilder.toString();
    }

}
