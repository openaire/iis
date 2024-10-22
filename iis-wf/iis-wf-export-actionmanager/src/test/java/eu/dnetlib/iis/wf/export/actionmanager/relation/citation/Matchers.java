package eu.dnetlib.iis.wf.export.actionmanager.relation.citation;

import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.KeyValue;
import eu.dnetlib.dhp.schema.oaf.Relation;

import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class Matchers {

    private Matchers() {
    }

    public static Matcher<Relation> matchingRelation(Relation relation) {
        return new TypeSafeMatcher<Relation>() {
            @Override
            protected boolean matchesSafely(Relation item) {
                return relation.getRelType().equals(item.getRelType()) &&
                        relation.getSubRelType().equals(item.getSubRelType()) &&
                        relation.getRelClass().equals(item.getRelClass()) &&
                        relation.getSource().equals(item.getSource()) &&
                        relation.getTarget().equals(item.getTarget()) &&
                        Float.parseFloat(relation.getDataInfo().getTrust()) == Float.parseFloat(item.getDataInfo().getTrust()) &&
                        relation.getDataInfo().getInferenceprovenance().equals(item.getDataInfo().getInferenceprovenance()) &&
                        matchesCollectedFrom(relation.getCollectedfrom(), item.getCollectedfrom());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("matching relation " + relation);
            }
        };
    }
    
    private static boolean matchesCollectedFrom(List<KeyValue> source, List<KeyValue> target) {
        if (source != null) {
            if (target != null) {
                if (source.size() == target.size()) {
                    for (int i = 0; i < source.size(); i++) {
                        if (!source.get(i).getKey().equals(target.get(i).getKey()) || 
                                !source.get(i).getValue().equals(target.get(i).getValue())) {
                            return false;
                        }
                    }
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            if (target == null) {
                return true;
            } else {
                return false;
            }
        }
    }

    public static Matcher<AtomicAction<Relation>> matchingAtomicAction(AtomicAction<Relation> atomicAction) {
        return new TypeSafeMatcher<AtomicAction<Relation>>() {
            @Override
            protected boolean matchesSafely(AtomicAction<Relation> item) {
                return atomicAction.getClazz().equals(item.getClazz()) &&
                        matchingRelation(atomicAction.getPayload()).matches(item.getPayload());
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("matching atomic action " + atomicAction);
            }
        };
    }
}
