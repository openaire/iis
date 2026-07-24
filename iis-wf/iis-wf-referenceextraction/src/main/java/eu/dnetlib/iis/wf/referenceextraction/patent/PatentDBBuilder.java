package eu.dnetlib.iis.wf.referenceextraction.patent;

import eu.dnetlib.iis.referenceextraction.patent.schemas.PatentReferenceExtractionInput;
import eu.dnetlib.iis.wf.referenceextraction.AbstractDBBuilder;

/**
 * Process building patents database reading {@link PatentReferenceExtractionInput} input avro records.
 *
 * @author mhorst
 */
public class PatentDBBuilder extends AbstractDBBuilder<PatentReferenceExtractionInput> {

    // -------------------------- CONSTRUCTORS ------------------------------

    public PatentDBBuilder() {
        super(PatentReferenceExtractionInput.SCHEMA$);
    }

}
