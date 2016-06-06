/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
        FSDataOutputStream out = fs.create(new Path("/user/"+hadoopUser + "/Hamond/" + key + ".out"));
        IOUtils.copyBytes(in, out, 4096, true);
        p2.waitFor();
    }
    
}
