/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package awshamondsidefunctions;

import java.io.File;
import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class CopyFromS3 {

    public static void copyFromS3(String diamond, String query, String dataBase) throws IOException, InterruptedException {
        String copyCommand1[] = {"aws", "s3", "cp", diamond, "/mnt/"};
        Process p1 = Runtime.getRuntime().exec(copyCommand1);
        p1.waitFor();

        String copyCommand2[] = {"aws", "s3", "cp", query, "/mnt/"};
        Process p2 = Runtime.getRuntime().exec(copyCommand2);
        p2.waitFor();

        String copyCommand3[] = {"aws", "s3", "cp", dataBase, "/mnt/"};
        Process p3 = Runtime.getRuntime().exec(copyCommand3);
        p3.waitFor();
        
        File file = new File("/mnt/diamond");
        file.setReadable(true, false);
        file.setExecutable(true, false);
        file.setWritable(true, false);

    }

}
