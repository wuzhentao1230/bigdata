/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregatorBaseDescriptor;
import org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregatorJob;

/**
 * This is an example Aggregated Hadoop Map/Reduce application. It reads the
 * text input files, breaks each line into words and counts them. The output is
 * a locally sorted list of words and the count of how often they occurred.
 * 
 * To run: bin/hadoop jar hadoop-*-examples.jar aggregatewordcount 
 * <i>in-dir</i> <i>out-dir</i> <i>numOfReducers</i> textinputformat
 *
 *
 这个是hadoop的map/reduce的例子，是对例子WordCount利用系统已经实现的map/reduce类进行简化。
 系统已经实现的ValueAggregatorBaseDescriptor
 和ValueAggregatorJob已经实现各种数据类型的求和最大值，最小值的算法。类型如下：
 UniqValueCount
 LongValueSum
 DoubleValueSum
 ValueHistogram
 LongValueMax
 LongValueMin
 StringValueMax
 StringValueMin
 具体请看相关的源代码。
 这个job的执行必须用-jarlibs执行，不然会报configured错误。
 执行命令如下：
 hadoop jar hadoop-example.jar -libjars hadoop-example.jar shakepoems.text out_aggregate_his 3 textinputformat

 */

public class AggregateWordCount {

  public static class WordCountPlugInClass extends
      ValueAggregatorBaseDescriptor {
    @Override
    public ArrayList<Entry<Text, Text>> generateKeyValPairs(Object key,
                                                            Object val) {
      String countType = LONG_VALUE_SUM;
      ArrayList<Entry<Text, Text>> retv = new ArrayList<Entry<Text, Text>>();
      String line = val.toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        Entry<Text, Text> e = generateEntry(countType, itr.nextToken(), ONE);
        if (e != null) {
          retv.add(e);
        }
      }
      return retv;
    }
  }

  /**
   * The main driver for word count map/reduce program. Invoke this method to
   * submit the map/reduce job.
   * 
   * @throws IOException
   *           When there is communication problems with the job tracker.
   */
  @SuppressWarnings("unchecked")
  public static void main(String[] args) 
    throws IOException, InterruptedException, ClassNotFoundException  {
    Job job = ValueAggregatorJob.createValueAggregatorJob(args
        , new Class[] {WordCountPlugInClass.class});
    job.setJarByClass(AggregateWordCount.class);
    int ret = job.waitForCompletion(true) ? 0 : 1;
    System.exit(ret);
  }

}
