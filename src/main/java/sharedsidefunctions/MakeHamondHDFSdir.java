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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author yujia1986
 */
public class MakeHamondHDFSdir {
    public static void makedir(Configuration conf, String userName) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path("/user/"+userName+"/Hamond");
        if (fs.exists(p)) {
            fs.delete(p, true);
        }
        fs.mkdirs(p);
        fs.close();
    }
}
