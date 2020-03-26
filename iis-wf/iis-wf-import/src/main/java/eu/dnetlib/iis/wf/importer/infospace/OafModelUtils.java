package eu.dnetlib.iis.wf.importer.infospace;

import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.Country;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.ExternalReference;
import eu.dnetlib.dhp.schema.oaf.ExtraInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.GeoLocation;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.OAIProvenance;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.OriginDescription;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

/**
 * @author mhorst
 *
 */
public class OafModelUtils {

    public static final Class<?>[] provideOafClasses() {
        // FIXME how to make this more flexible?
        return new Class[]{
                Author.class,
                Context.class,
                Country.class,
                DataInfo.class,
                Dataset.class,
                Datasource.class,
                ExternalReference.class,
                ExtraInfo.class,
                Field.class,
                GeoLocation.class,
                Instance.class,
                Journal.class,
                KeyValue.class,
                Oaf.class,
                OafEntity.class,
                OAIProvenance.class,
                Organization.class,
                OriginDescription.class,
                OtherResearchProduct.class,
                Project.class,
                Publication.class,
                Qualifier.class,
                Relation.class,
                Result.class,
                Software.class,
                StructuredProperty.class
        };
    }
    
}
