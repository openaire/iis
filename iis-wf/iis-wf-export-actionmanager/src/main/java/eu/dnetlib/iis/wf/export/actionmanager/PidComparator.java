package eu.dnetlib.iis.wf.export.actionmanager;

import java.util.Comparator;

import eu.dnetlib.dhp.schema.common.ModelSupport;
import eu.dnetlib.dhp.schema.oaf.OafEntity;
import eu.dnetlib.dhp.schema.oaf.Organization;
import eu.dnetlib.dhp.schema.oaf.Result;
import eu.dnetlib.dhp.schema.oaf.StructuredProperty;

public class PidComparator<T extends OafEntity> implements Comparator<StructuredProperty> {

    private final T entity;

    public PidComparator(T entity) {
        this.entity = entity;
    }

    @Override
    public int compare(StructuredProperty left, StructuredProperty right) {

        if (left == null && right == null)
            return 0;
        if (left == null)
            return 1;
        if (right == null)
            return -1;

        if (ModelSupport.isSubClass(entity, Result.class)) {
            return compareResultPids(left, right);
        }
        
        if (ModelSupport.isSubClass(entity, Organization.class)) {
            return compareOrganizationtPids(left, right);
        }

        // Else (but unlikely), lexicographical ordering will do.
        return left.getQualifier().getClassid().compareTo(right.getQualifier().getClassid());
    }

    private int compareResultPids(StructuredProperty left, StructuredProperty right) {
        return new ResultPidComparator().compare(left, right);
    }
    
    private int compareOrganizationtPids(StructuredProperty left, StructuredProperty right) {
        return new OrganizationPidComparator().compare(left, right);
    }
}