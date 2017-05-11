package eu.dnetlib.iis.wf.export.actionmanager.module;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.InvalidProtocolBufferException;

import eu.dnetlib.data.proto.KindProtos;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.RelTypeProtos.RelType;
import eu.dnetlib.data.proto.RelTypeProtos.SubRelType;
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
    public static void assertOafRel(byte[] oafBytes, Expectations expectations) throws InvalidProtocolBufferException {
        
        assertNotNull(oafBytes);
        Oaf.Builder oafBuilder = Oaf.newBuilder();
        oafBuilder.mergeFrom(oafBytes);
        Oaf oaf = oafBuilder.build();
        assertNotNull(oaf);

        assertTrue(expectations.getKind() == oaf.getKind());
        assertTrue(expectations.getRelType() == oaf.getRel().getRelType());
        assertTrue(expectations.getSubRelType() == oaf.getRel().getSubRelType());
        assertEquals(expectations.getRelationClass(), oaf.getRel().getRelClass());
        assertEquals(expectations.getSource(), oaf.getRel().getSource());
        assertEquals(expectations.getTarget(), oaf.getRel().getTarget());    
        assertNotNull(oaf.getDataInfo());

        float normalizedTrust = expectations.getConfidenceLevel() * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR;
        assertEquals(normalizedTrust, Float.parseFloat(oaf.getDataInfo().getTrust()), 0.0001);
    }
    
    // --------------------------------- INNER CLASS -----------------------------------

    public static class Expectations {
        
        private String source;
        private String target;
        private float confidenceLevel;
        private KindProtos.Kind kind;
        private RelType relType;
        private SubRelType subRelType;
        private String relationClass;
        
        public Expectations(String source, String target, float confidenceLevel, Kind kind, RelType relType,
                SubRelType subRelType, String relationClass) {
            this.source = source;
            this.target = target;
            this.confidenceLevel = confidenceLevel;
            this.kind = kind;
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

        public KindProtos.Kind getKind() {
            return kind;
        }

        public RelType getRelType() {
            return relType;
        }

        public SubRelType getSubRelType() {
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

        public void setKind(KindProtos.Kind kind) {
            this.kind = kind;
        }

        public void setRelType(RelType relType) {
            this.relType = relType;
        }

        public void setSubRelType(SubRelType subRelType) {
            this.subRelType = subRelType;
        }

        public void setRelationClass(String relationClass) {
            this.relationClass = relationClass;
        }
    }
    
}
