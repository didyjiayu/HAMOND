package awshamondsidefunctions;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import sharedsidefunctions.HadoopUser;

/**
 *
 * @author yujia1986
 */
public class CopyToS3 {

    public static void copyToS3(String outPut) throws IOException, InterruptedException {

        String userName = HadoopUser.getHadoopUser();
        //get output file name
        String outputName = new Path(outPut).getName();
        Path outputPath = new Path(outPut).getParent();
        
        
//        String copyCommand[] = {"hadoop", "fs", "-getmerge", userName+"/*.out", System.getProperty("user.dir")+"/"+query+".out"};
//        String copyCommand1[] = {"hadoop", "fs", "-getmerge", "Hamond/*.out", "/mnt/Hamond.out"};
//        Process p1 = Runtime.getRuntime().exec(copyCommand1);
//        p1.waitFor();
        
//        String copyCommand2[] = {"aws", "s3", "cp", "/mnt/Hamond.out", outPut};
//        Process p2 = Runtime.getRuntime().exec(copyCommand2);
//        p2.waitFor();

        //use stream merge all output files back into a single outpuf file in HDFS
        String copyCommand1[] = {"bash", "-c", "hadoop fs -text Hamond/*.out | hadoop fs -put - Hamond/"+outputName};
        Process p1 = Runtime.getRuntime().exec(copyCommand1);
        p1.waitFor();
        
        //mapreduce single output file back to s3 using user specified output file name
        String copyCommand2[] = {"hadoop", "distcp", "Hamond/"+outputName, outputPath.toString()};
        Process p2 = Runtime.getRuntime().exec(copyCommand2);
        p2.waitFor();
        
    }

}
