package eu.dnetlib.iis.wf.export.actionmanager.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.InfoSpaceConstants;

/**
 * Utility methods useful for assertion validation.
 * @author mhorst
 *
 */
public final class VerificationUtils {

    
    // ------------------------------- CONSTRUCTORS -----------------------------
    
    private VerificationUtils() {}

    
    // ------------------------------- LOGIC ------------------------------------


    /**
     * Evaluates oafBytes against expectations.
     */
    public static void assertOafRel(Relation relation, Expectations expectations) throws InvalidProtocolBufferException {
        assertNotNull(relation);

        assertEquals(expectations.getRelType(), relation.getRelType());
        assertEquals(expectations.getSubRelType(), relation.getSubRelType());
        assertEquals(expectations.getRelationClass(), relation.getRelClass());
        assertEquals(expectations.getSource(), relation.getSource());
        assertEquals(expectations.getTarget(), relation.getTarget());    
        assertNotNull(relation.getDataInfo());

        float normalizedTrust = expectations.getConfidenceLevel() * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
        assertEquals(normalizedTrust, Float.parseFloat(relation.getDataInfo().getTrust()), 0.0001);
    }
    
    // --------------------------------- INNER CLASS -----------------------------------

    public static class Expectations {
        
        private String source;
        private String target;
        private float confidenceLevel;
        private String relType;
        private String subRelType;
        private String relationClass;
        
        public Expectations(String source, String target, float confidenceLevel, String relType,
                String subRelType, String relationClass) {
            this.source = source;
            this.target = target;
            this.confidenceLevel = confidenceLevel;
            this.relType = relType;
            this.subRelType = subRelType;
            this.relationClass = relationClass;
        }

        public String getSource() {
            return source;
        }

        public String getTarget() {
            return target;
        }

        public float getConfidenceLevel() {
            return confidenceLevel;
        }

        public String getRelType() {
            return relType;
        }

        public String getSubRelType() {
            return subRelType;
        }

        public String getRelationClass() {
            return relationClass;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public void setTarget(String target) {
            this.target = target;
        }

        public void setConfidenceLevel(float confidenceLevel) {
            this.confidenceLevel = confidenceLevel;
        }

        public void setRelType(String relType) {
            this.relType = relType;
        }

        public void setSubRelType(String subRelType) {
            this.subRelType = subRelType;
        }

        public void setRelationClass(String relationClass) {
            this.relationClass = relationClass;
        }
    }
    
}
