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

package org.apache.giraph.examples;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.examples.TopicIntArrayListWritable;
import org.apache.giraph.examples.TopicVocabMessageCustomeAccumlateVariance;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.giraph.edge.EdgeFactory;
import java.util.LinkedList;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.giraph.aggregators.AggregateMessageCustome;
import org.apache.giraph.aggregators.VarianceAggregatorOptimizedAccumlate;
/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
    name = "Topic Detection",
    description = "Detect Topics"
)
public class TopicVertexVocabBasedOptimizedAggregateAccumlateVariance extends Vertex<LongWritable, DoubleWritable, FloatWritable, TopicVocabMessageCustomeAccumlateVariance> {
                                    					//vertex ID type, vertex value type, edge value type, message value type

  private static String VAR_AGG = "VarianceAggregatorOptimized";
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(TopicVertex.class);

  private float TWEET_SIZE = 20000;
  private float epson = 0.01f;
  private double counter = 0;
  private static final int C = 1000;
  public static final int SUPERSTEP0_MSG = 0;
  public static final int SUPERSTEP1_MSG = 1;
  public static final int SUPERSTEP2_MSG = 2;

  //private double edgesthreshold = 0;
  
  /*
  //Dynamic run instead of shutting down and re-running Giraph
  public static final long WINDOW_SIZE = 20000;
  public static final long SHIFT_SIZE = 10000;
  public static final long MEGA_STEP = 4;
  public static final String BATCH_FILE_NAME = "/user/exp/ahmed/batches/";
  public static final long TOTAL_TWEETS = 110000;
  */
  /*
   * Is this vertex the source id?
   *
   * @return True if the source id
  *private boolean isSource() {
  *  return getId().get() == SOURCE_ID.get(getConf());
  *}
  */
  @Override
  public void compute(Iterable<TopicVocabMessageCustomeAccumlateVariance> messages) {
	
    if(getSuperstep() == 0){  //Each vocab will broadcast its neighbor IDs
        if(getValue().equals(new DoubleWritable(0))){ //0 for vocab
			int edgesNum = getNumEdges();
            for (Edge<LongWritable, FloatWritable> edge : getEdges()){
                sendMessage(edge.getTargetVertexId(), new TopicVocabMessageCustomeAccumlateVariance((int)getId().get(), edgesNum, SUPERSTEP0_MSG)); 
            }
			voteToHalt();
         }
    } 
	
	else if(getSuperstep() == 1){
        if(getValue().equals(new DoubleWritable(1))){ //1 for tweet
		  HashMap<Integer, Float> tfidfs = new HashMap<Integer, Float> ();
		  for (Edge<LongWritable, FloatWritable> edge : getEdges()){
			int vocabId = (int)edge.getTargetVertexId().get();
			float tf = edge.getValue().get();
			tfidfs.put(vocabId, tf);
		  }
		  int nodeid = (int)getId().get();
		  int[] vocabids = new int[tfidfs.size()];
		  float[] vocabtfidf = new float[tfidfs.size()];
		  int index = 0;
		  for (TopicVocabMessageCustomeAccumlateVariance message : messages) {
              int vocabId = message.getSourceId();
              int vocabDegree = message.getDegreeNum();
			  float tf = tfidfs.get(vocabId);
			  float tfidf = (tf * TWEET_SIZE) / (vocabDegree);
			  vocabids[index] = vocabId;
			  vocabtfidf[index++] = tfidf;
          }
		  
		  TopicVocabMessageCustomeAccumlateVariance tfidfVectorMsg = new TopicVocabMessageCustomeAccumlateVariance(nodeid, vocabids, vocabtfidf, SUPERSTEP1_MSG);
		  for (Edge<LongWritable, FloatWritable> edge : getEdges()){
			sendMessage(edge.getTargetVertexId(), tfidfVectorMsg); 
		  }
		  voteToHalt();
        }	
    } else if(getSuperstep() == 2){
		if(getValue().equals(new DoubleWritable(0))){ // vocab
			try{
			int nodeid = (int)getId().get();
//			FileSystem fs = FileSystem.get(new Configuration());
//			Path ptlog = new Path("/user/exp/ahmed/test_"+nodeid+".txt");
//			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(ptlog,true)));
		
			HashMap<Integer, HashMap<Integer, Float>> tweets = new HashMap<Integer, HashMap<Integer, Float>> ();
			for (TopicVocabMessageCustomeAccumlateVariance message : messages) {
				int tweetId = message.getSourceId();
				int[] vocabs = message.getNeighborId();
				float[] tfidfs = message.getTFIDF();
				HashMap<Integer, Float> tweetTFIDF = new HashMap<Integer, Float> ();
//				br.write(tweetId+" ");
				for(int i=0; i < vocabs.length; i++) {
					tweetTFIDF.put(vocabs[i], tfidfs[i]);
//					br.write(vocabs[i]+":"+tfidfs[i]+" ");
				}
//				br.newLine();
				tweets.put(tweetId, tweetTFIDF);
			}
			
			for(Integer tweet1 : tweets.keySet()) {
				LinkedList<Integer> conflicList = new LinkedList<Integer>();
				LinkedList<Float> conflictValues = new LinkedList<Float>();
				float xsquare = 0.0f;
				float xsum = 0.0f;
				int conflictCount = 0;
				for(Integer tweet2 : tweets.keySet()){
					if(tweet1 == tweet2)
						continue;
					HashMap<Integer, Float> tfidf1 = tweets.get(tweet1);
					HashMap<Integer, Float> tfidf2 = tweets.get(tweet2);
					float sim = 0.0f;
					float norm1 = 0.0f;
					float norm2 = 0.0f;
					boolean report = true;
					for(Integer vocabId : tfidf1.keySet()) {
						float vocabTFIDF1 = tfidf1.get(vocabId);
						if(tfidf2.containsKey(vocabId)) {
							sim += vocabTFIDF1 * tfidf2.get(vocabId);
							if(vocabId > nodeid) {
								report = false;
								break;
							}
						}
						norm1 += vocabTFIDF1 * vocabTFIDF1;
					}
					if(!report)
						continue;
					
					for(Integer vocabId : tfidf2.keySet()) {
						float vocabTFIDF2 = tfidf2.get(vocabId);
						norm2 += vocabTFIDF2 * vocabTFIDF2;
					}
					norm1 = (float) Math.sqrt(norm1);
					norm2 = (float) Math.sqrt(norm2);
					sim = sim /(norm1*norm2);
					if( sim >= epson){
						if(conflictCount < C) {
							int index = 0;
							boolean done = false;
							for(Float conflictSim : conflictValues) {
								if(sim > conflictSim) {
									conflictValues.add(index, sim);
									conflicList.add(index, tweet2);
									done = true;
									break;
								}								
								index++;
							}
							if(!done) {
								conflictValues.add(sim);
								conflicList.add(tweet2);
							}
							conflictCount++;
						} else {
							int index = 0;
							boolean done = false;
							for(Float conflictSim : conflictValues) {
								if(sim > conflictSim) {
									conflictValues.add(index, sim);
									conflicList.add(index, tweet2);
									done = true;
									break;
								}								
								index++;
							}
							if(done) {
								conflicList.removeLast();
								conflictValues.removeLast();
							}
						}
					}
//					br.write(tweet1+"\t"+tweet2+"\t"+sim);
//					br.newLine();
					xsquare += sim*sim;
					xsum += sim;
				}
				int[] newConflictList = new int[conflictCount];
				int index = 0;
				for(Integer id: conflicList) {
					newConflictList[index++] = id;
				}
				if (xsum == 0 && xsquare == 0)
					continue;
				sendMessage(new LongWritable(new Long(tweet1)) , new TopicVocabMessageCustomeAccumlateVariance(newConflictList, xsquare, xsum,  SUPERSTEP2_MSG));
			}
//			br.close();
			} catch(Exception ex) {
			}
			voteToHalt();
		}
    } else if(getSuperstep() == 3){
		if(getValue().equals(new DoubleWritable(1))){ //1 for tweet
		try{
			float xsquareSum = 0.0f;
			float xsum = 0.0f;
//			FileSystem fs = FileSystem.get(new Configuration());
			int nodeid = (int)getId().get();
//			Path ptlog = new Path("/user/exp/ahmed/test_"+nodeid+".txt");
//			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(ptlog,true)));
		
			HashSet<Integer> conflictList = new HashSet<Integer>();
			for (TopicVocabMessageCustomeAccumlateVariance message : messages) {
				xsquareSum += message.getXsquareSum();
				xsum += message.getXsum();
//				br.write("Msg x^2 = "+xsquareSum);
//				br.newLine();
//				br.write("Msg x = "+xsum);
//				br.newLine();
				int[] conflicts = message.getConflictList();
				for(int conflict : conflicts) {
					conflictList.add(conflict);
//					br.write("Msg Conflict = "+conflict);
//					br.newLine();
				}
			}
			float avg = xsum / TWEET_SIZE;
			float var = (xsquareSum - 2*avg*xsum + avg*avg*TWEET_SIZE)/TWEET_SIZE;
//			br.write("Msg avg = "+avg);
//			br.newLine();
//			br.write("Msg var = "+var);
//			br.newLine();
//			br.close();
			//Collections.sort(conflictList);
			AggregateMessageCustome msg = new AggregateMessageCustome((int)getId().get(), conflictList, var);//, numMegaSetps);
			aggregate(VarianceAggregatorOptimizedAccumlate.VAR_AGG, msg); 
			}catch(Exception ex) {
			}
		}
		voteToHalt();
	}
  }

  /**
   * Master compute associated with {@link TopicVertex}.
   * It registers required aggregators.
   */
  public static class SimplePageRankVertexMasterCompute extends DefaultMasterCompute {
      @Override
      public void initialize() throws InstantiationException,
          IllegalAccessException {
        //registerAggregator("VarianceAggregator", VarianceAggregator.class);
        //registerPersistentAggregator(MIN_AGG, DoubleMinAggregator.class);
        //registerPersistentAggregator(MAX_AGG, DoubleMaxAggregator.class);
      }
    }
}


