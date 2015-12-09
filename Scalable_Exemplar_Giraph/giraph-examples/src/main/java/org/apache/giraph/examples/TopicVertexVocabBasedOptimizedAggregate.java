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
import org.apache.giraph.examples.TopicVocabMessage;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.giraph.edge.EdgeFactory;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.giraph.aggregators.AggregateMessageCustome;
import org.apache.giraph.aggregators.VarianceAggregatorOptimized;
/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
    name = "Topic Detection",
    description = "Detect Topics"
)
public class TopicVertexVocabBasedOptimizedAggregate extends Vertex<LongWritable, DoubleWritable, FloatWritable, TopicVocabMessageCustome> {
                                    					//vertex ID type, vertex value type, edge value type, message value type

  private static String VAR_AGG = "VarianceAggregatorOptimized";
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(TopicVertex.class);

  private float TWEET_SIZE = 20000;
  private float epson = 0.01f;
  private double counter = 0;
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
  public void compute(Iterable<TopicVocabMessageCustome> messages) {
	
    if(getSuperstep() == 0){  //Each vocab will broadcast its neighbor IDs
        if(getValue().equals(new DoubleWritable(0))){ //0 for vocab
			int edgesNum = getNumEdges();
			int[] neighbors = new int[edgesNum+1];
			neighbors[0] = edgesNum;
			float[] tfidfs = new float[edgesNum+1];
			tfidfs[0] = edgesNum;
            for (Edge<LongWritable, FloatWritable> edge : getEdges()){
				long vertexid = edge.getTargetVertexId().get();
				int index = 1;
				for (Edge<LongWritable, FloatWritable> edge2 : getEdges()) {
					long neighbourid = edge2.getTargetVertexId().get();
				    float edgevalue = edge2.getValue().get();
					float tfidf = (edgevalue*TWEET_SIZE) / edgesNum;
					//if (vertexid == neighbourid)
						//continue;
					tfidfs[index] = tfidf;
					neighbors[index++] = (int) neighbourid;
				}
                sendMessage(edge.getTargetVertexId(), new TopicVocabMessageCustome((int)getId().get(), neighbors, tfidfs)); 
            }
			voteToHalt();
         }
    } 
	
	
	else if(getSuperstep() == 1){
        if(getValue().equals(new DoubleWritable(1))){ //1 for tweet
		  setValue(new DoubleWritable(0.0));
		  HashMap<Integer, Float> tfidfs = new HashMap<Integer, Float> ();
          HashMap<Integer, HashMap<Integer, Float>> neighborTweets = new HashMap<Integer, HashMap<Integer, Float>>(); //The tweets that share at least one 
		  int nodeid = (int)getId().get();
		  float norm = 0f;
		  for (TopicVocabMessageCustome message : messages) {
              int vocabId = message.getSourceId();
              int[] tweetId = message.getNeighborId();
			  float[] msgtfidfs = message.getTFIDF();
			  for(int i = 1; i < tweetId.length; i++){
			  if(nodeid == tweetId[i]) {
		  		norm += msgtfidfs[i] * msgtfidfs[i];
				tfidfs.put(vocabId, msgtfidfs[i]);

			  }
			  else {
			  HashMap<Integer, Float> neighbourTweetsVocab = neighborTweets.get(tweetId[i]);
				if (neighbourTweetsVocab == null) {
					neighbourTweetsVocab = new HashMap<Integer, Float>();
					neighborTweets.put(tweetId[i], neighbourTweetsVocab);
				}
				neighbourTweetsVocab.put(vocabId, msgtfidfs[i]);
			  }
			  }
          }
		  norm = (float)Math.sqrt(norm);
		  int nsize = neighborTweets.size();
		  for(Integer neighbor : neighborTweets.keySet()) {
			float cosSim = 0.0f;
		    HashMap<Integer, Float> currentTweetTFIDF = neighborTweets.get(neighbor);
			for(Integer mytfidf : tfidfs.keySet()) {
					if(currentTweetTFIDF.containsKey(mytfidf)){
						cosSim += currentTweetTFIDF.get(mytfidf) * tfidfs.get(mytfidf);
					}
			}
			cosSim = cosSim/norm;
			//if(nsize >= edgesthreshold) {
				addEdge(EdgeFactory.create(new LongWritable(neighbor), new FloatWritable(cosSim)));
			//}
			int[] sentNeighbors = new int[1];
			float[] senttfidf = new float[1];
			senttfidf[0] = norm;
			sendMessage(new LongWritable(new Long(neighbor)), new TopicVocabMessageCustome((int)getId().get(),sentNeighbors, senttfidf)); 
		  }
		  
		  //setValue(new DoubleWritable(newvar));

        }
		
    } else if(getSuperstep() == 2){
		  //if(getValue().equals(new DoubleWritable(1))){ //1 for tweet
			//Calculate similarity with other tweets that share at least one vocab with the current tweet
			HashMap<Integer, Float> norms = new HashMap<Integer, Float> ();
			for (TopicVocabMessageCustome message : messages) {
				 int vocabId = message.getSourceId();
				 float[] tfidfs = message.getTFIDF();
				 norms.put(vocabId, tfidfs[0]);
			}
		//if(norms.size() >= edgesthreshold) {
			ArrayList<Float> sims = new ArrayList<Float> ();
			HashSet<Integer> neighbors = new HashSet<Integer>();
			for (MutableEdge<LongWritable, FloatWritable> edge : getMutableEdges()){
				int myid = (int)edge.getTargetVertexId().get();
				if(myid <= 0) // vocab vertex
					continue;
				float norm = norms.get(myid);		
				float sim = edge.getValue().get();
				sim = sim /norm;
				sims.add(sim);
				if (sim >= epson)
					neighbors.add(myid);			
				//edge.setValue(new FloatWritable(sim));
			}
			 //Calculate the variance
			 float var = getVariance(sims);
			 setValue(new DoubleWritable(var));
			 //int numMegaSetps = (int)(getSuperstep() / (MEGA_STEP-1));
			 //Collections.sort(neighbors);
			 AggregateMessageCustome msg = new AggregateMessageCustome((int)getId().get(), neighbors, var);//, numMegaSetps);
			 aggregate(VarianceAggregatorOptimized.VAR_AGG, msg); 
		// }
		 voteToHalt();
      //}
    }

  }

  float getMean(ArrayList<Float> data)
    {
        float sum = 0.0f;
        for(Float a : data)
            sum += a;
        return sum/TWEET_SIZE;
    }

    float getVariance(ArrayList<Float> data)
    {
        float mean = getMean(data);
        float temp = 0f;
        for(Float a :data)
            temp += (mean-a)*(mean-a);
        temp += (mean*mean)  * (TWEET_SIZE - data.size());
        return temp/TWEET_SIZE;
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


