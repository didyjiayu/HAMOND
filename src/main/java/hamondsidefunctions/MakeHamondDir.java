package hamondsidefunctions;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author yujia1986
 */
public class MakeHamondDir {
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
