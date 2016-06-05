package awshamondsidefunctions;

import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class DeleteHDFSFiles {

    public static void deleteAllFiles(String userName) throws IOException, InterruptedException {
        
        String[] deleteFiles = {"hadoop", "fs", "-rm", "-r", "/user/"+userName+"/Hamond"};
        Process delete = Runtime.getRuntime().exec(deleteFiles);
        delete.waitFor();
    }

}
