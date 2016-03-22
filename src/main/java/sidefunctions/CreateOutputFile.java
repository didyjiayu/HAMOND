package sidefunctions;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author yujia1986
 */
public class CreateOutputFile {
    
    public static void create(String userName, FileSystem fs, String query) throws IOException {
        if (fs.exists(new Path(userName+"/"+query+".out"))) {
            fs.delete(new Path(userName+"/"+query+".out"), false);
        }
        fs.create(new Path(userName+"/"+query+".out"));
    }
    
}
