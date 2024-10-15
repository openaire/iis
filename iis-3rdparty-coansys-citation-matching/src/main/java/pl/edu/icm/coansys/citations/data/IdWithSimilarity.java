package pl.edu.icm.coansys.citations.data;

import java.io.Serializable;

/**
 * Class representing identifier with similarity
 * 
 * @author madryk
 *
 */
public class IdWithSimilarity implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    
    private String id;
    private double similarity;
    
    
    //------------------------ CONSTRUCTORS --------------------------
    
    public IdWithSimilarity(String id, double similarity) {
        this.id = id;
        this.similarity = similarity;
    }
    
    
    //------------------------ GETTERS --------------------------
    
    public double getSimilarity() {
        return similarity;
    }

    public String getId() {
        return id;
    }

}
