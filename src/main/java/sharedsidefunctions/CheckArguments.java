/*
 * Copyright 2016 YU Jia

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package sharedsidefunctions;

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
        
        if(!argumentsList.get(1).contains(".f")||!argumentsList.get(2).contains(".f")) {
            throw new IllegalArgumentException("You have to give query and reference genome!");
        }
        
        if(argumentsList.size()<5||(!argumentsList.get(4).contains("blastp")&&!argumentsList.get(4).contains("blastx"))) {
            throw new IllegalArgumentException("You have to specify BLAST type after output argument!");
        }
        
        
    }

}
