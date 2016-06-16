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


import java.io.File;
import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class MakeDB {

    public static void makeDB(String diamond, String dataBase) throws IOException, InterruptedException {
        
        String command[] = {new File(diamond).getAbsolutePath(), "makedb", "--in", dataBase, "-d", dataBase};
        Process p = Runtime.getRuntime().exec(command);
        p.waitFor();
        
    }

}
