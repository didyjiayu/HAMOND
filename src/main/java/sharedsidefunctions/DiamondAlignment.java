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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author yujia1986
 */
public class DiamondAlignment {

    public static void align(String diamond, String localDB, String key, String[] arguments) throws IOException, InterruptedException {
        ArrayList<String> argumentsList = new ArrayList<String>(Arrays.asList(arguments));
        String qda[] = {"-q", "/tmp/" + key, "-d", localDB, "-a", "/tmp/" + key};
        argumentsList.add(0, diamond);
        argumentsList.addAll(new ArrayList<String>(Arrays.asList(qda)));
        Process p1 = Runtime.getRuntime().exec(argumentsList.toArray(new String[argumentsList.size()]));
        p1.waitFor();
    }

}
