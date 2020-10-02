package eu.dnetlib.iis.wf.importer.infospace.converter;

import eu.dnetlib.iis.common.schemas.IdentifierMapping;
import org.junit.jupiter.api.BeforeEach;

/**
 * Tests for {@link DeduplicationMappingConverter}.
 */
public class DeduplicationMappingConverterTest extends OafRelToAvroConverterTestBase<IdentifierMapping> {

    @BeforeEach
    public void setUp() {
        converter = new DeduplicationMappingConverter();
        getSourceId = IdentifierMapping::getNewId;
        getTargetId = IdentifierMapping::getOriginalId;
    }

    // ------------------------ TESTS --------------------------
    // All tests are in the base class
}
