package sidefunctions;


import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class makeDB {

    public static void makeDB(String diamond, String input) throws IOException, InterruptedException {
        String command[] = {diamond, "makedb", "--in", input, "-d", input};
        Process p = Runtime.getRuntime().exec(command);
        p.waitFor();
    }

}
