/*
 * Copyright 2016 YU Jia

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

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
