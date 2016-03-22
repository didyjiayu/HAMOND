package sidefunctions;


import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class MakeDB {

    public static void makeDB(String diamond, String query) throws IOException, InterruptedException {
        String command[] = {diamond, "makedb", "--in", query, "-d", query};
        Process p = Runtime.getRuntime().exec(command);
        p.waitFor();
    }

}
