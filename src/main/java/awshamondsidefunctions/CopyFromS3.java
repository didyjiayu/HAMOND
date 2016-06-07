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

package awshamondsidefunctions;

import java.io.File;
import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class CopyFromS3 {

    public static void copyFromS3(String diamond, String query, String dataBase) throws IOException, InterruptedException {
        String copyCommand1[] = {"aws", "s3", "cp", diamond, "/mnt/Hamond"};
        Process p1 = Runtime.getRuntime().exec(copyCommand1);
        p1.waitFor();

        String copyCommand2[] = {"aws", "s3", "cp", query, "/mnt/Hamond"};
        Process p2 = Runtime.getRuntime().exec(copyCommand2);
        p2.waitFor();

        String copyCommand3[] = {"aws", "s3", "cp", dataBase, "/mnt/Hamond"};
        Process p3 = Runtime.getRuntime().exec(copyCommand3);
        p3.waitFor();
        
        File file = new File("/mnt/Hamond/diamond");
        file.setReadable(true, false);
        file.setExecutable(true, false);
        file.setWritable(true, false);

    }

}
