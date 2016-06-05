/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hamondsidefunctions;

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
        conf.set("mapreduce.map.memory.mb", "8192");
        conf.set("mapreduce.reduce.memory.mb", "8192");
        Logger.getLogger("amazon.emr.metrics").setLevel(Level.OFF);

    }

}
