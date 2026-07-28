package eu.dnetlib.iis.wf.referenceextraction.researchinitiative;

import eu.dnetlib.iis.referenceextraction.researchinitiative.schemas.ResearchInitiativeMetadata;
import eu.dnetlib.iis.wf.referenceextraction.AbstractDBBuilder;

/**
 * Process building research initiatives database reading {@link ResearchInitiativeMetadata} input avro records.
 *
 * @author mhorst
 */
public class ResearchInitiativeDBBuilder extends AbstractDBBuilder<ResearchInitiativeMetadata> {

    // -------------------------- CONSTRUCTORS ------------------------------

    public ResearchInitiativeDBBuilder() {
        super(ResearchInitiativeMetadata.SCHEMA$);
    }

}
