package eu.dnetlib.iis.wf.export.actionmanager.module;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.iis.common.InfoSpaceConstants;
import eu.dnetlib.iis.common.model.conversion.ConfidenceAndTrustLevelConversionUtils;
import eu.dnetlib.iis.wf.export.actionmanager.cfg.StaticConfigurationProvider;

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
    private static final DecimalFormat decimalFormat = initailizeDecimalFormat();
    
    /**
     * Creates {@link Relation} initialized with basic metadata.
     * 
     * @param source          relation source
     * @param target          relation target
     * @param relType         relation type
     * @param subRelType      relation sub-type
     * @param relClass        relation class
     * @param dataInfo        {@link DataInfo} describing relation
     * @param collectedFromValue relation collectedfrom value
     */
    public static Relation createRelation(String source, String target, String relType, String subRelType,
            String relClass, DataInfo dataInfo, String collectedFromValue) {
        return createRelation(source, target, relType, subRelType, relClass, null, dataInfo, collectedFromValue);
    }
    
    /**
     * Creates {@link Relation} initialized with basic metadata.
     * 
     * @param source          relation source
     * @param target          relation target
     * @param relType         relation type
     * @param subRelType      relation sub-type
     * @param relClass        relation class
     * @param properties      relation properties
     * @param dataInfo        {@link DataInfo} describing relation
     * @param collectedFromValue relation collectedfrom value
     */
    public static Relation createRelation(String source, String target, String relType, String subRelType,
            String relClass, List<KeyValue> properties, DataInfo dataInfo, String collectedFromValue) {
        Relation relation = new Relation();
        relation.setSource(source);
        relation.setTarget(target);
        relation.setRelType(relType);
        relation.setSubRelType(subRelType);
        relation.setRelClass(relClass);
        relation.setProperties(properties);
        relation.setDataInfo(dataInfo);
        relation.setLastupdatetimestamp(System.currentTimeMillis());
        relation.setCollectedfrom(Collections.singletonList(buildCollectedFromKeyValue(collectedFromValue)));
        return relation;
    }
    
    /**
     * Returns {@link KeyValue} object with the predefined key and value provided as parameter.
     * @param collectedFromValue value of {@link KeyValue} object
     */
    public static KeyValue buildCollectedFromKeyValue(String collectedFromValue) {
        KeyValue keyValue = new KeyValue();
        keyValue.setKey(StaticConfigurationProvider.COLLECTED_FROM_KEY);
        keyValue.setValue(collectedFromValue);
        return keyValue;
    }
    
    /**
     * Returns {@link DataInfo} with inference details including inferred flag set to true.
     * 
     */
    public static DataInfo buildInferenceForConfidenceLevel(
            float confidenceLevel, String inferenceProvenance) {
        return buildInferenceForTrustLevel(
                decimalFormat.format(ConfidenceAndTrustLevelConversionUtils.confidenceLevelToTrustLevel(confidenceLevel)),
                inferenceProvenance);
    }
    
    /**
     * Returns {@link DataInfo} with inference details including inferred flag set to true.
     * 
     */
    public static DataInfo buildInferenceForTrustLevel(
            String trustLevel, String inferenceProvenance) {
        return buildInferenceForTrustLevel(true, trustLevel, inferenceProvenance, InfoSpaceConstants.SEMANTIC_CLASS_IIS);
    }

    /**
     * Returns {@link DataInfo} with inference details.
     * 
     */
    public static DataInfo buildInferenceForTrustLevel(boolean inferred, 
            String trustLevel, String inferenceProvenance, String provenanceClass) {
        DataInfo dataInfo = new DataInfo();
        dataInfo.setInferred(inferred);
        dataInfo.setTrust(trustLevel);
        Qualifier provenanceQualifier = new Qualifier();
        provenanceQualifier.setClassid(provenanceClass);
        provenanceQualifier.setClassname(provenanceClass);
        provenanceQualifier.setSchemeid(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS);
        provenanceQualifier.setSchemename(InfoSpaceConstants.SEMANTIC_SCHEME_DNET_PROVENANCE_ACTIONS);
        dataInfo.setProvenanceaction(provenanceQualifier);
        dataInfo.setInferenceprovenance(inferenceProvenance);
        return dataInfo;
    }
    
    /**
     * Provides predefined decimal format to be used for representing float values as String. 
     */
    public static DecimalFormat getDecimalFormat() {
        return decimalFormat;
    }
    
    // ----------------------------------- PRIVATE ----------------------------------
    
    private static DecimalFormat initailizeDecimalFormat() {
        DecimalFormat decimalFormat = (DecimalFormat) NumberFormat.getInstance(Locale.ENGLISH);
        decimalFormat.applyPattern("#.####");
        return decimalFormat;
    }
    
}
