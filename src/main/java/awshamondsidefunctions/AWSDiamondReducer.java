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
package awshamondsidefunctions;

import diamondmapreduce.DiamondMapReduce;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import sharedsidefunctions.HadoopUser;

/**
 *
 * @author yujia1986
 */
public class AWSDiamondReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String userName = HadoopUser.getHadoopUser();
        Configuration conf = context.getConfiguration();
        String outPut = conf.get(DiamondMapReduce.OUTPUT);
        FileSystem fs = FileSystem.get(conf);
        //merge all output files into one output file on HDFS
        FileUtil.copyMerge(fs, new Path("/user/" + userName + "/Hamond/output"), fs, new Path("/user/" + userName + "/Hamond/"+new Path(outPut).getName()), false, conf, null);
    }

}
