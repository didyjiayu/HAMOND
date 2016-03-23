package sidefunctions;

import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class DeleteHDFSFiles {

    public static void deleteAllFiles(String query) throws IOException, InterruptedException {
        
        String userName = HadoopUser.getHadoopUser();
        String[] deleteFiles = {"hadoop", "fs", "-rm", "-r", userName + "/"+query+"*", userName + "/output", "diamond"};
        Process delete = Runtime.getRuntime().exec(deleteFiles);
        delete.waitFor();
    }

}
