package sidefunctions;

import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class DeleteHDFSFiles {

    public static void deleteAllFiles(String userName) throws IOException, InterruptedException {
        
        String[] deleteFiles = {"hadoop", "fs", "-rm", "-r", userName + "/*.f*", userName+"/*.dmnd", userName+"/output", userName+"/diamond", userName+"/*.out"};
        Process delete = Runtime.getRuntime().exec(deleteFiles);
        delete.waitFor();
    }

}
