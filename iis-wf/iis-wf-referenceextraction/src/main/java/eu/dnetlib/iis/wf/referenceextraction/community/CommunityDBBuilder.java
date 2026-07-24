package eu.dnetlib.iis.wf.referenceextraction.community;

import eu.dnetlib.iis.referenceextraction.community.schemas.Community;
import eu.dnetlib.iis.wf.referenceextraction.AbstractDBBuilder;

/**
 * Process creating communities database reading {@link Community} input avro records.
 *
 * @author mhorst
 */
public class CommunityDBBuilder extends AbstractDBBuilder<Community> {

    // -------------------------- CONSTRUCTORS ------------------------------

    public CommunityDBBuilder() {
        super(Community.SCHEMA$);
    }

}
