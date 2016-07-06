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

package diamondmapreduce;

/**
 *
 * @author yujia1986
 */
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import sharedsidefunctions.DiamondAlignment;
import sharedsidefunctions.DeleteIntermediateFiles;
import sharedsidefunctions.DiamondView;
import sharedsidefunctions.WriteKeyValueToTemp;

public class DiamondMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String localDB;
    private String diamond;

    //get local path of DIAMOND and database
    @Override
    public void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path[] local = context.getLocalCacheFiles();
        this.diamond = local[0].toString();
        this.localDB = local[1].toString();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {

        //get query and database name from mapreduce driver
        Configuration conf = context.getConfiguration();
        String query = conf.get(DiamondMapReduce.QUERY);
        String dataBase = conf.get(DiamondMapReduce.DATABASE);
        String[] args = conf.getStrings("DIAMOND-arguments");

        //write key-value pair to local tmp
        WriteKeyValueToTemp.write(key.toString(), value.toString());

        //use runtime to execute alignment, intermediate binary files are stored in local tmp
        DiamondAlignment.align(this.diamond, this.localDB, key.toString(), args, conf);

        //view the binary files to tabular output file, view output will be streammized into HDFS
//        DiamondView.view(this.diamond, key.toString(), conf);
        
        //delete all intermediate files
        DeleteIntermediateFiles.deleteFiles(key.toString());
        
        context.write(new Text("key"), new Text(key.toString()));

    }
}
