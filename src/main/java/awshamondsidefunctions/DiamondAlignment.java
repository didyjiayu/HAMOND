package awshamondsidefunctions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author yujia1986
 */
public class DiamondAlignment {

    public static void align(String diamond, String localDB, String key, String[] arguments) throws IOException, InterruptedException {
        ArrayList<String> argumentsList = new ArrayList<String>(Arrays.asList(arguments));
        String qda[] = {"-q", "/tmp/" + key, "-d", localDB, "-a", "/tmp/" + key};
        argumentsList.add(0, diamond);
        argumentsList.addAll(new ArrayList<String>(Arrays.asList(qda)));
        Process p1 = Runtime.getRuntime().exec(argumentsList.toArray(new String[argumentsList.size()]));
        p1.waitFor();
    }

}
