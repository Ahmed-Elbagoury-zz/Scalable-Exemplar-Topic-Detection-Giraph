/*
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

package org.apache.giraph.aggregators;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.giraph.aggregators.BasicAggregator;
import java.util.ArrayList;
import org.apache.giraph.aggregators.Tweet;

import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.fs.FSDataInputStream;

public class VarianceAggregatorOptimizedAccumlate extends BasicAggregator<AggregateMessageCustome> {
  //private ArrayList<Integer> vertexes;
  //private ArrayList<int[]> neighborList;
  //private ArrayList<float> variance;

  //private ArrayList<Tweet> tweets;
  AggregateMessageCustome aggregateMessageCustome;
  public static final int topicNumber = 10;
  public static final String VAR_AGG = "VarianceAggregatorOptimizedAccumlate.java";
  public static final int factor = 3000;
  public static final int workerNum = 3;
  public static int stage = 0;
  /*
  public VarianceAggregator(){
    super();
	tweets = new ArrayList<Tweet>();
    //vertexes = new ArrayList<Integer> ();
    //neighborList = new ArrayList<ArrayList<Integer>> ();
    //variance = new ArrayList<float> ();
  }
  */
  
  @Override
  public void aggregate(AggregateMessageCustome value) {
  
	//int megaStepInd = value.getMegaSlotInd();
	//if(value.getTweetList().get(0).getId() == -1)
	//	return;
    //if(neighborList == null)
	//	neighborList = new ArrayList<int[]> ();
	//if(simList == null)
	//	variance = new ArrayList<float> ();
	//if(tweets == null)
	//	tweets = new ArrayList<Tweet>();
	LinkedList<Tweet> tweetList = value.getTweetList();
	LinkedList<Tweet> newList = new LinkedList<Tweet>();

	if(tweetList.size() == 1) { // worker aggregate
		AggregateMessageCustome prev = getAggregatedValue();
		newList = prev.getTweetList();
		Tweet currTweet = tweetList.get(0);
		newList.add(currTweet);
	} 
	if (tweetList.size() > 1) { // master aggregate
		AggregateMessageCustome prev = getAggregatedValue();
		newList = prev.getTweetList();
		for(Tweet tweet: tweetList){
			newList.add(tweet);
		}
		stage++;
		System.out.println("stage value = " + stage);
	}

	getAggregatedValue().setTweetList(newList);
	//getAggregatedValue().setMegaSlotInd(megaStepInd);
	if(tweetList.size() > 1 && (stage % workerNum == 0) && (stage != 0) ){
		try{
			//System.out.println("About to write in the aggregator");
			//Path pt = new Path("/user/exp/ahmed/20k_10k/output/timeslot-0");
			//int numMegaStep = (stage/workerNum) - 1;
			long starttime = System.currentTimeMillis();
			FileSystem fs = FileSystem.get(new Configuration());
//			Path ptlog = new Path("/user/exp/ahmed/master"+System.currentTimeMillis()+".txt");
//			BufferedWriter brlog = new BufferedWriter(new OutputStreamWriter(fs.create(ptlog,true)));
		
			//FileStatus[] fileStatus = fs.listStatus(new Path("/user/exp/ahmed/50k_sliding_20k_output"));
			//Path[] paths = FileUtil.stat2Paths(fileStatus);
			//int numMegaStep = paths.length;
			//System.out.println("Current numMegaStep = "+numMegaStep);
			String outFileName = "/user/exp/ahmed/timeslot.txt";
			Path pt = new Path(outFileName);
			System.out.println("Writing output to " + outFileName + ". pt = " + pt);
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			Collections.sort(newList);
			/*
			String varFileName = "/user/exp/ahmed/20k_sliding_10k_output/var-"+ numMegaStep + ".txt";
			Path varpt = new Path(varFileName);
			System.out.println("Writing output to " + varFileName + ". pt = " + varpt);
			BufferedWriter varbr = new BufferedWriter(new OutputStreamWriter(fs.create(varpt,true)));

			for(Tweet tweet : newList){ 
				varbr.write(tweet.getId()+"\t"+tweet.getVariance());
				varbr.newLine();
			}
			varbr.close();
			*/
			HashSet<Integer> selectedTweets = new HashSet<Integer>();
			int lastTopic = -1;
			int topicCount = 0;
			Iterator<Tweet> tweetItr = newList.iterator();
			Tweet nextTweet = tweetItr.next();
			
			Loop: while (topicCount < topicNumber) {
				//br.write(nextTweet.getId()+"\t"+nextTweet.getVariance());
				//br.newLine();
				while ((nextTweet.getConflictList().contains(lastTopic))
					&& (lastTopic != -1)) {
					if (!tweetItr.hasNext())
						break Loop;
					nextTweet = tweetItr.next();
					//br.write(nextTweet.getId()+"\t"+nextTweet.getVariance()+hashSetToString(nextTweet.getConflictList()));
					//br.newLine();
				}
				//br.write(nextTweet.getId()+"\t"+nextTweet.getVariance()+hashSetToString(nextTweet.getConflictList()));
				//br.newLine();
				lastTopic = nextTweet.getId();
				selectedTweets.add(lastTopic);
				if (!tweetItr.hasNext())
					break;
				nextTweet = tweetItr.next();
				topicCount++;
			}
			// get tweet text
			String fileName = "/user/exp/ahmed/tweets-0";
			Path ptRead = new Path(fileName);
			FSDataInputStream reader = fs.open(ptRead); 
			System.out.println("In agg. Getting tweet text from " + fileName + ". reader = " + reader);
			String line = null;
			//System.out.println("Reader created " + reader + ", content = " + reader.readLine());
			while ((line = reader.readLine()) != null) {
				String[] splits = line.split("\t");
				int id = Integer.parseInt(splits[0]);
				//System.out.println("About to write");
				if (selectedTweets.contains(id) && splits.length >= 2) {
					//System.out.println(splits[1]);
					br.write(splits[1]);
					br.newLine();
				}
			}
			reader.close();
			br.close();
			long diffTime = System.currentTimeMillis() - starttime;
//			brlog.write(diffTime+"");
//			brlog.newLine();
//			brlog.close();
			
			  }
		  catch(IOException ex){
				  System.out.println("In aggregator: " + ex.getMessage());
		  }
    }


	//vertexes.add(vertexId);
	//neighborList.add(neighbor);
	//variance.add(value.getVar());
	//System.out.println(tweets.size());
  }

  public String  hashSetToString (ArrayList<Integer> set) {
	StringBuilder sb = new StringBuilder();
	for(Integer item : set) 
		sb.append(" "+item);
	return sb.toString();  
	
  }

  @Override
  public AggregateMessageCustome createInitialValue() {
  	//ArrayList<Tweet> list = new ArrayList<Tweet>();
  	//Tweet tweet = new Tweet(-1, 0.0f, new HashSet<Integer>());
  	//list.add(tweet);
  	this.aggregateMessageCustome = new AggregateMessageCustome(new LinkedList<Tweet>());
  	return this.aggregateMessageCustome;
    //return new AggregateMessageCustome(-1, new HashSet<Integer>(), 0.0f);
  }
/*
  @Override
  public AggregateMessageCustome getAggregatedValue(){
  	ArrayList<Tweet> tweetCopy = new ArrayList<Tweet>();
  	for(Tweet tweet: this.aggregateMessageCustome.getTweetList()){
  		Tweet copyTweet = new Tweet(tweet.getId(), tweet.getVariance(), tweet.getConflictList());
  		tweetCopy.add(copyTweet);
  	}
  	AggregateMessageCustome aggregateMessageCustome = new AggregateMessageCustome(tweetCopy);
  	//System.out.println(aggregateMessageCustome.getTweetList().size());
  	return aggregateMessageCustome;
  }
  */
}
  
