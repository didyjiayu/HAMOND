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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 *
 * @author yujia1986
 */
public class DiamondAlignment {

    public static void align(String diamond, String localDB, String key, String[] arguments, Configuration conf) throws IOException, InterruptedException {
        ArrayList<String> argumentsList = new ArrayList<String>(Arrays.asList(arguments));
//        String qda[] = {"-q", "/tmp/" + key, "-d", localDB, "-a", "/tmp/" + key};
        String qda[] = {"-q", "/tmp/" + key, "-d", localDB};
        argumentsList.add(0, diamond);
        argumentsList.addAll(new ArrayList<String>(Arrays.asList(qda)));
//        Process p1 = Runtime.getRuntime().exec(argumentsList.toArray(new String[argumentsList.size()]));
//        p1.waitFor();
        String hadoopUser = UserGroupInformation.getCurrentUser().getUserName();
        Process p = Runtime.getRuntime().exec(argumentsList.toArray(new String[argumentsList.size()]));
        FileSystem fs = FileSystem.get(conf);
        //process stream copied to HDFS stream
        InputStream in = p.getInputStream();
        FSDataOutputStream out = fs.create(new Path("/user/"+hadoopUser + "/Hamond/output/" + key + ".out"));
        IOUtils.copyBytes(in, out, 4096, true);
        p.waitFor();
        
    }

}
