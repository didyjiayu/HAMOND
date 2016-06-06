package sharedsidefunctions;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 *
 * @author yujia1986
 */
public class WriteKeyValueToTemp {
    
    public static void write(String key, String value) throws IOException {
        FileWriter file = new FileWriter("/tmp/" + key);
        try (BufferedWriter bf = new BufferedWriter(file)) {
            bf.write(value);
            bf.close();
        }
//        file.write(value.toString());
        file.close();
    }
    
}
