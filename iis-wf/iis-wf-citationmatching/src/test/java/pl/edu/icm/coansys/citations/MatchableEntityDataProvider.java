package pl.edu.icm.coansys.citations;

import java.util.List;

import com.google.common.collect.Lists;

import pl.edu.icm.coansys.citations.data.MatchableEntity;
import scala.Tuple2;

/**
 * @author madryk
 */
public class MatchableEntityDataProvider {

    public static MatchableEntity citation1 = MatchableEntity.fromParameters("cit_AAA_46",
            "Peeters, R.", "Combinatorica", "Orthogonal representations over finite fields and the chromatic number of graphs", "417-431", "1996",
            "[46] R. Peeters, “Orthogonal representations over finite fields and the chromatic number of graphs,” Combinatorica, vol. 16, no. 3, pp. 417-431, 1996.");
    
    public static MatchableEntity citation2 = MatchableEntity.fromParameters("cit_AAA_48",
            "Dimakis, A. G., Vontobel, P. O., Sensing, Compressed", "in Proc. of Allerton", "LP Decoding meets LP Decoding: A Connection between Channel Coding", null, "2009",
            "[48] A. G. Dimakis and P. O. Vontobel, “LP Decoding meets LP Decoding: A Connection between Channel Coding and Compressed Sensing,” in Proc. of Allerton, 2009.");
    
    public static MatchableEntity citation3 = MatchableEntity.fromParameters("cit_AAA_44",
            "Ourivski, A. V., Johansson, T.", null, "New technique for decoding codes in the rank metric and its cryptography applications", "237-246", "July 2002",
            "[44] A. V. Ourivski and T. Johansson, “New technique for decoding codes in the rank metric and its cryptography applications,” Probl. Inf. Transm., vol. 38, pp. 237-246, July 2002.");
    
    public static MatchableEntity citation4 = MatchableEntity.fromParameters("cit_AAB_3",
            "Fedor, V., Fomin, Yngve, Villanger", "In STACS", "Finding induced subgraphs via minimal triangulations", "383-394", "2010", 
            "Fedor V. Fomin and Yngve Villanger. Finding induced subgraphs via minimal triangulations. In STACS, pages 383-394, 2010.");
    
    public static MatchableEntity citation5 = MatchableEntity.fromParameters("cit_AAB_4",
            "Gottlob, Georg, Leone, Nicola, Scarcello, Francesco", "J. Comput. Syst. Sci.", "Hypertree decompositions and tractable queries", "579-627", "2002",
            "Georg Gottlob, Nicola Leone, and Francesco Scarcello. Hypertree decompositions and tractable queries. J. Comput. Syst. Sci., 64(3):579-627, 2002.");
    
    
    
    public static MatchableEntity document1 = MatchableEntity.fromParameters("doc_BBA",
            "Peeters, I (Imre)", null, "The oxidative ammonolysis of ethylene to acetonitrile over supported molybdenum catalysts", null, "1996", null);
    
    public static MatchableEntity document2 = MatchableEntity.fromParameters("doc_BBB",
            "Peeters, M.J.P.", null, "Orthogonal Representations over Finite Fields and the Chromatic Number of Graphs", null, "1996", null);
    
    public static MatchableEntity document3 = MatchableEntity.fromParameters("doc_BBC1",
            "Peeters, E.T.H.M., Klein, J.J.M., de", null, "Gebiedsvreemd water in de IJsselvallei.", null, "1996", null);
    
    public static MatchableEntity document3v2 = MatchableEntity.fromParameters("doc_BBC2",
            "Peeters", null, "one two three four", null, "1996", null);
    
    public static MatchableEntity document4 = MatchableEntity.fromParameters("doc_BBD",
            "Smarandache, Roxana, Vontobel, Pascal O.", "July", "Absdet-Pseudo-Codewords and Perm-Pseudo-Codewords: Definitions and Properties", null, "2009", null);
    
    public static MatchableEntity document5 = MatchableEntity.fromParameters("doc_BBE",
            "Fomin, Fedor V., Villanger, Yngve", null, "Finding Induced Subgraphs via Minimal Triangulations", "382-394", "2010", null);
    
    
    //------------------------ LOGIC --------------------------
    
    public static List<Tuple2<String, MatchableEntity>> generateIdWithEntityTuples(List<MatchableEntity> entities) {
        List<Tuple2<String, MatchableEntity>> entitiesWritable = Lists.newArrayList();
        
        for (MatchableEntity entity : entities) {
            entitiesWritable.add(new Tuple2<String, MatchableEntity>(entity.id(), entity));
        }
        
        return entitiesWritable;
    }
    
}
