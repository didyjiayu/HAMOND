package hamondsidefunctions;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * 
 * @author yujia1986
 */
public class CheckArguments {
    
    public static void check(String[] arguments) {
        
        ArrayList<String> argumentsList = new ArrayList<String>(Arrays.asList(arguments));
        
        if(!argumentsList.get(0).contains("diamond")) {
            throw new IllegalArgumentException("You have to first give the path of DIAMOND!");
        }
        
        if(!argumentsList.get(1).contains(".faa")||!argumentsList.get(2).contains(".faa")) {
            throw new IllegalArgumentException("You have to give query and reference genome!");
        }
        
        if(argumentsList.size()<5||(!argumentsList.get(4).contains("blastp")&&!argumentsList.get(4).contains("blastx"))) {
            throw new IllegalArgumentException("You have to specify BLAST type after output argument!");
        }
        
        
    }

}
