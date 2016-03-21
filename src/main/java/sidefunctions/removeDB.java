package sidefunctions;

import java.io.File;

/**
 *
 * @author yujia1986
 */
public class removeDB {
    
    public static void removeDB (String db) {
        File file = new File(db);
        file.delete();
    }
    
}
