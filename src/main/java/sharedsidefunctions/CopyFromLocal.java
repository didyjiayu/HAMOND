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
public class CopyFromLocal {
    
    public static void copyFromLocal(Configuration conf, String diamond, String query, String dataBase, String userName) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path(dataBase + ".dmnd"), new Path("/user/"+userName+"/Hamond"));
        fs.copyFromLocalFile(new Path(query), new Path("/user/"+userName+"/Hamond"));
//        fs.copyFromLocalFile(new Path(System.getProperty("user.dir")+"/diamond"), new Path(userName));
        fs.copyFromLocalFile(new Path(diamond), new Path("/user/"+userName+"/Hamond"));
        
        //close file system
        fs.close();
    }
    
}
