package hamondsidefunctions;

import java.io.IOException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 *
 * @author yujia1986
 */
public class HadoopUser {
    public static String getHadoopUser() throws IOException {
        String hadoopUser = UserGroupInformation.getCurrentUser().getUserName();
        return hadoopUser;
    }
}
