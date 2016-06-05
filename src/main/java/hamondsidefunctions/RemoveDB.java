package hamondsidefunctions;

import java.io.File;

/**
 *
 * @author yujia1986
 */
public class RemoveDB {
    
    public static void removeDB (String db) {
        File file = new File(db);
        file.deleteOnExit();
    }
    
}
