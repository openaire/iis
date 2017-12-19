package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

import eu.dnetlib.data.proto.FieldTypeProtos.DataInfo;
import eu.dnetlib.data.proto.FieldTypeProtos.Qualifier;
import eu.dnetlib.data.proto.KindProtos.Kind;
import eu.dnetlib.data.proto.OafProtos.Oaf;
import eu.dnetlib.data.proto.OafProtos.OafEntity;
import eu.dnetlib.data.proto.OafProtos.OafRel;
import eu.dnetlib.data.proto.RelMetadataProtos.RelMetadata;
import eu.dnetlib.iis.common.InfoSpaceConstants;

/**
 * {@link Oaf} builder helper.
 * 
 * @author mhorst
 *
 */
public class BuilderModuleHelper {

    /**
     * Trust level format.
     */
    private final static DecimalFormat decimalFormat = (DecimalFormat) NumberFormat.getInstance(Locale.ENGLISH);
    
    {
        decimalFormat.applyPattern("#.####");
    }
    
    /**
     * Builds {@link Oaf} object for given entity and inference details.
     * 
     */
    public static Oaf buildOaf(OafEntity oafEntity, float confidenceLevel, String inferenceProvenance) {
        return buildOaf(oafEntity, buildInferenceForConfidenceLevel(confidenceLevel, inferenceProvenance));
    }
    
    /**
     * Builds {@link Oaf} object for given entity and data description.
     * 
     */
    public static Oaf buildOaf(OafEntity oafEntity, DataInfo dataInfo) {
        eu.dnetlib.data.proto.OafProtos.Oaf.Builder oafBuilder = Oaf.newBuilder();
        oafBuilder.setKind(Kind.entity);
        oafBuilder.setEntity(oafEntity);
        if (dataInfo != null) {
            oafBuilder.setDataInfo(dataInfo);
        }
        oafBuilder.setLastupdatetimestamp(System.currentTimeMillis());
        return oafBuilder.build();
    }
    
    /**
     * Builds relation metadata.
     * 
     */
    public static RelMetadata buildRelMetadata(String schemaId, String classId) {
        RelMetadata.Builder relBuilder = RelMetadata.newBuilder();
        Qualifier.Builder qBuilder = Qualifier.newBuilder();
        qBuilder.setSchemeid(schemaId);
        qBuilder.setSchemename(schemaId);
        qBuilder.setClassid(classId);
        qBuilder.setClassname(classId);
        relBuilder.setSemantics(qBuilder.build());
        return relBuilder.build();
    }
    
    /**
     * Returns {@link DataInfo} with inference details.
     * 
     */
    public static DataInfo buildInferenceForConfidenceLevel(
            float confidenceLevel, String inferenceProvenance) {
        return buildInferenceForTrustLevel(
                decimalFormat.format(confidenceLevel * InfoSpaceConstants.CONFIDENCE_TO_TRUST_LEVEL_FACTOR),
                inferenceProvenance);
    }
    
    /**
     * Returns {@link DataInfo} with inference details.
     * 
     */
    public static DataInfo buildInferenceForTrustLevel(
            String trustLevel, String inferenceProvenance) {
        DataInfo.Builder builder = DataInfo.newBuilder();
        builder.setInferred(true);
        builder.setTrust(trustLevel);
        Qualifier.Builder provenanceBuilder = Qualifier.newBuilder();
        provenanceBuilder.setClassid(InfoSpaceConstants.SEMANTIC_CLASS_IIS);
        provenanceBuilder.setClassname(InfoSpaceConstants.SEMANTIC_CLASS_IIS);
        provenanceBuilder.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS);
        provenanceBuilder.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS);
        builder.setProvenanceaction(provenanceBuilder.build());
        builder.setInferenceprovenance(inferenceProvenance);
        return builder.build();
    }
    
    /**
     * Clones builder provided as parameter, inverts relations and builds new Oaf object. 
     * Relation direction is not iverted as it is bidirectional.
     * 
     * @param existingBuilder {@link Oaf.Builder} to be cloned
     * @return Oaf object containing relation with inverted source and target fields.
     */
    public static Oaf invertBidirectionalRelationAndBuild(Oaf.Builder existingBuilder) {
        // works on builder clone to prevent changes in existing builder
        if (existingBuilder.getRel() != null) {
            if (existingBuilder.getRel().getSource() != null && existingBuilder.getRel().getTarget() != null) {
                Oaf.Builder builder = existingBuilder.clone();
                OafRel.Builder relBuilder = builder.getRelBuilder();
                String source = relBuilder.getSource();
                String target = relBuilder.getTarget();
                relBuilder.setSource(target);
                relBuilder.setTarget(source);
                builder.setRel(relBuilder.build());
                builder.setLastupdatetimestamp(System.currentTimeMillis());
                return builder.build();
            } else {
                throw new RuntimeException("invalid state: " + "either source or target relation was missing!");
            }
        } else {
            throw new RuntimeException("invalid state: " + "no relation object found!");
        }
    }

    /**
     * Provides predefined decimal format to be used for representing float values as String. 
     */
    public static DecimalFormat getDecimalFormat() {
        return decimalFormat;
    }
    
}
