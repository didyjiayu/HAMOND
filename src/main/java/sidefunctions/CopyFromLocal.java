/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sidefunctions;

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
