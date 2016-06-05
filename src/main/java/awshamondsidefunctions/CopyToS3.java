package awshamondsidefunctions;

import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class CopyToS3 {

    public static void copyToS3(String outPut) throws IOException, InterruptedException {

        String userName = HadoopUser.getHadoopUser();
//        String copyCommand[] = {"hadoop", "fs", "-getmerge", userName+"/*.out", System.getProperty("user.dir")+"/"+query+".out"};
//        String copyCommand1[] = {"hadoop", "fs", "-getmerge", "Hamond/*.out", "/mnt/Hamond.out"};
//        Process p1 = Runtime.getRuntime().exec(copyCommand1);
//        p1.waitFor();
        
//        String copyCommand2[] = {"aws", "s3", "cp", "/mnt/Hamond.out", outPut};
//        Process p2 = Runtime.getRuntime().exec(copyCommand2);
//        p2.waitFor();

        String copyCommand1[] = {"bash", "-c", "hadoop fs -text Hamond/*.out | hadoop fs -put - Hamond/Hamond.out"};
        Process p1 = Runtime.getRuntime().exec(copyCommand1);
        p1.waitFor();
        
        String copyCommand2[] = {"hadoop", "distcp", "Hamond/Hamond.out", outPut};
        Process p2 = Runtime.getRuntime().exec(copyCommand2);
        p2.waitFor();
        
    }

}
