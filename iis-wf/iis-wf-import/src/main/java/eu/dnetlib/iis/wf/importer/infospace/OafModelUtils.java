package eu.dnetlib.iis.wf.importer.infospace;

import eu.dnetlib.dhp.schema.oaf.AccessRight;
import eu.dnetlib.dhp.schema.oaf.Author;
import eu.dnetlib.dhp.schema.oaf.Context;
import eu.dnetlib.dhp.schema.oaf.Country;
import eu.dnetlib.dhp.schema.oaf.DataInfo;
import eu.dnetlib.dhp.schema.oaf.Dataset;
import eu.dnetlib.dhp.schema.oaf.Datasource;
import eu.dnetlib.dhp.schema.oaf.EoscIfGuidelines;
import eu.dnetlib.dhp.schema.oaf.ExternalReference;
import eu.dnetlib.dhp.schema.oaf.ExtraInfo;
import eu.dnetlib.dhp.schema.oaf.Field;
import eu.dnetlib.dhp.schema.oaf.GeoLocation;
import eu.dnetlib.dhp.schema.oaf.H2020Classification;
import eu.dnetlib.dhp.schema.oaf.H2020Programme;
import eu.dnetlib.dhp.schema.oaf.Instance;
import eu.dnetlib.dhp.schema.oaf.InstanceTypeMapping;
import eu.dnetlib.dhp.schema.oaf.Journal;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Measure;
import eu.dnetlib.dhp.schema.oaf.OAIProvenance;
import eu.dnetlib.dhp.schema.oaf.Oaf;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.OpenAccessColor;
import eu.dnetlib.dhp.schema.oaf.OpenAccessRoute;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.OriginDescription;
import eu.dnetlib.dhp.schema.oaf.OtherResearchProduct;
import eu.dnetlib.dhp.schema.oaf.Person;
import eu.dnetlib.dhp.schema.oaf.PersonTopic;
import eu.dnetlib.dhp.schema.oaf.Pid;
import eu.dnetlib.dhp.schema.oaf.Project;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.oaf.Qualifier;
import eu.dnetlib.dhp.schema.oaf.RawAuthorAffiliation;
import eu.dnetlib.dhp.schema.oaf.Relation;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.Software;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;
import eu.dnetlib.dhp.schema.oaf.Subject;

/**
 * @author mhorst
 *
 */
public class OafModelUtils {

    /**
     * Provides all classes from Oaf model.
     */
    public static final Class<?>[] provideOafClasses() {
        return new Class[]{
                AccessRight.class,
                Author.class,
                Context.class,
                Country.class,
                DataInfo.class,
                Dataset.class,
                Datasource.class,
                EoscIfGuidelines.class,
                ExternalReference.class,
                ExtraInfo.class,
                Field.class,
                GeoLocation.class,
                H2020Classification.class,
                H2020Programme.class,
                Instance.class,
                InstanceTypeMapping.class,
                Journal.class,
                KeyValue.class,
                Measure.class,
                Oaf.class,
                OafEntity.class,
                OAIProvenance.class,
                OpenAccessColor.class,
                OpenAccessRoute.class,
                Organization.class,
                OriginDescription.class,
                OtherResearchProduct.class,
                Person.class,
                PersonTopic.class,
                Pid.class,
                Project.class,
                Publication.class,
                Qualifier.class,
                RawAuthorAffiliation.class,
                Relation.class,
                Result.class,
                Software.class,
                StructuredProperty.class,
                Subject.class
        };
    }
    
}
