package eu.dnetlib.iis.wf.importer.infospace.converter;

import org.junit.Before;

import eu.dnetlib.iis.common.schemas.IdentifierMapping;

/**
 * Tests for {@link DeduplicationMappingConverter}.
 */
public class DeduplicationMappingConverterTest extends OafRelToAvroConverterTestBase<IdentifierMapping> {

    @Before
    public void setUp() {
        converter = new DeduplicationMappingConverter();
        getSourceId = IdentifierMapping::getNewId;
        getTargetId = IdentifierMapping::getOriginalId;
    }

    // ------------------------ TESTS --------------------------
    // All tests are in the base class
}
