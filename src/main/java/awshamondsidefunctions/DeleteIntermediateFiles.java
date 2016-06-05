package awshamondsidefunctions;

import java.io.File;

/**
 *
 * @author yujia1986
 */
public class DeleteIntermediateFiles {
    
    public static void deleteFiles(String key) {
        File file1 = new File("/tmp/" + key);
        File file2 = new File("/tmp/" + key+".daa");
        file1.deleteOnExit();
        file2.deleteOnExit();
        
    }
    
}
