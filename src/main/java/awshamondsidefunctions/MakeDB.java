package awshamondsidefunctions;


import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class MakeDB {

    public static void makeDB(String diamond, String dataBase) throws IOException, InterruptedException {
        
        String command[] = {diamond, "makedb", "--in", dataBase, "-d", dataBase};
        Process p = Runtime.getRuntime().exec(command);
        p.waitFor();
        
    }

}
