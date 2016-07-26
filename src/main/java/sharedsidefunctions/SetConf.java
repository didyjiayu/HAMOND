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

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author yujia1986
 */
public class SetConf {

    public static void setHadoopConf(Configuration conf) {
        conf.set("mapreduce.task.timeout", "36000000");
        conf.set("mapreduce.map.memory.mb", "5632");
        conf.set("mapreduce.reduce.memory.mb", "5632");
        conf.set("mapreduce.map.java.opts", "-Xmx5632M");
        conf.set("mapreduce.reduce.java.opts", "-Xmx5632M");
        //set reducer to wait until all mappers are finished
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1.0");
        Logger.getLogger("amazon.emr.metrics").setLevel(Level.OFF);

    }

}
