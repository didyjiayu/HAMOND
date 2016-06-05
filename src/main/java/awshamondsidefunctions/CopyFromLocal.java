package awshamondsidefunctions;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author yujia1986
 */
public class CopyFromLocal {
    
    public static void copyFromLocal(Configuration conf, String diamond, String query, String dataBase) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        fs.copyFromLocalFile(new Path(dataBase + ".dmnd"), new Path("Hamond/"));
        fs.copyFromLocalFile(new Path(query), new Path("Hamond/"));
//        fs.copyFromLocalFile(new Path(System.getProperty("user.dir")+"/diamond"), new Path(userName));
        fs.copyFromLocalFile(new Path(diamond), new Path("Hamond/"));
        
        //close file system
        fs.close();
    }

}
