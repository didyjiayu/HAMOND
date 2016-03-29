package sidefunctions;

import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class DiamondAlignment {

    public static void align(String diamond, String localDB, String key) throws IOException, InterruptedException {
        String alignment[] = {diamond, "blastp", "-q", "/tmp/" + key, "-d", localDB, "-a", "/tmp/" + key, "--seg", "no", "--sensitive", "-k", "30000", "-e", "0.00001"};
        Process p1 = Runtime.getRuntime().exec(alignment);
        p1.waitFor();
    }

}
