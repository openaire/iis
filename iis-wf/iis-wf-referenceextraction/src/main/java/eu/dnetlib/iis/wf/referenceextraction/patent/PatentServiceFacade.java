package eu.dnetlib.iis.wf.referenceextraction.patent;

import java.io.Serializable;
import java.util.NoSuchElementException;

import eu.dnetlib.iis.referenceextraction.patent.schemas.ImportedPatent;

/**
 * Patent Service facade.
 * 
 * @author mhorst
 *
 */
public interface PatentServiceFacade extends Serializable {

    /**
     * Retrieves patent metadata.
     * @throws NoSuchElementException when no metadata found for given patent
     */
    String getPatentMetadata(ImportedPatent patent) throws Exception;
}
