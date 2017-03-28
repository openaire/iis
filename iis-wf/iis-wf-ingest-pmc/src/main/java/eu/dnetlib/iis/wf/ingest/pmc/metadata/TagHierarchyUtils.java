package eu.dnetlib.iis.wf.ingest.pmc.metadata;

import java.util.Stack;

/**
 * Utils class with helper methods to look through xml parents tag stack
 * 
 * @author mhorst
 * @author madryk
 *
 */
public final class TagHierarchyUtils {

    //------------------------ CONSTRUCTOS --------------------------
    
    private TagHierarchyUtils() { }
    
    
    //------------------------ LOGIC --------------------------
    
    /**
     * Returns true only if current element is same as expectedElement
     */
    public static boolean isElement(String qName, String expectedElement) {
        return qName.equals(expectedElement);
    }
    
    /**
     * Returns true only if current element is same as expectedElement
     * and have direct parent defined in expectedParent
     */
    public static boolean isWithinElement(String qName,
            String expectedElement, Stack<String> parentStack, String expectedParent) {
        return qName.equals(expectedElement) && 
                (expectedParent==null || !parentStack.isEmpty() && expectedParent.equals(parentStack.peek()));
    }
    
    /**
     * Returns true if current element is same as expectedElement
     * and have among parents all of expectedParents (internally uses {@link #hasAmongParents(Stack, String...)}).
     */
    public static boolean hasAmongParents(String qName,
            String expectedElement, Stack<String> parentStack, String... expectedParents) {
        if (qName.equals(expectedElement)) {
            return hasAmongParents(parentStack, expectedParents);
        } else {
            return false;
        }
    }
    
    /**
     * Returns true if all of expectedParents are found in parentStack.
     * Order of expectedParents is relevant. Method will return true
     * only if they appear in parentStack in the same order (looking from the top)
     * as in expectedParents, but there could be gaps between them.
     * 
     */
    public static boolean hasAmongParents(Stack<String> parentStack, String... expectedParents) {
        if (expectedParents.length <= parentStack.size()) {
            int startIterationIdx = 0;
            for (String currentParent : expectedParents) {
                boolean found = false;
                for (int i=startIterationIdx; i<parentStack.size(); i++) {
//                  iteration starts from the bottom while we want to check from top
                    if (currentParent.equals(parentStack.get(parentStack.size()-(i+1)))) {
                        startIterationIdx = i+1;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    
}
