package hamondsidefunctions;

import sharedsidefunctions.HadoopUser;
import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class CopyToLocal {

    public static void copyToLocal(String outPut) throws IOException, InterruptedException {

        String userName = HadoopUser.getHadoopUser();
//        String copyCommand[] = {"hadoop", "fs", "-getmerge", userName+"/*.out", System.getProperty("user.dir")+"/"+query+".out"};
        String copyCommand[] = {"hadoop", "fs", "-getmerge", "/user/" + userName + "/Hamond/*.out", outPut};
        Process p = Runtime.getRuntime().exec(copyCommand);
        p.waitFor();

    }

}
