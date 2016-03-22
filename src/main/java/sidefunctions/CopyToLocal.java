package sidefunctions;

import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class CopyToLocal {
    
    public static void copyToLocal(String query) throws IOException, InterruptedException {
        
        String userName = HadoopUser.getHadoopUser();
        String copyCommand[] = {"hadoop", "fs", "-copyToLocal", userName+"/"+query+".out", System.getProperty("user.dir")};
        Process p = Runtime.getRuntime().exec(copyCommand);
        p.waitFor();
    
}
    
}
