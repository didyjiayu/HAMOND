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
public class DiamondView {
    
    public static void view(String diamond, String key, Configuration conf) throws IOException, InterruptedException {
        String hadoopUser = UserGroupInformation.getCurrentUser().getUserName();
        String view[] = {diamond, "view", "-a", "/tmp/" + key};
        Process p2 = Runtime.getRuntime().exec(view);
        FileSystem fs = FileSystem.get(conf);
        //process stream copied to HDFS stream
        InputStream in = p2.getInputStream();
        FSDataOutputStream out = fs.create(new Path("/user/"+hadoopUser + "/Hamond/output/" + key + ".out"));
        IOUtils.copyBytes(in, out, 4096, true);
        p2.waitFor();
    }
    
}
