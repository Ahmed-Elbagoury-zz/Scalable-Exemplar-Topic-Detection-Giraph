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
/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
    name = "Topic Detection",
    description = "Detect Topics"
)
public class TopicVertexShared extends Vertex<LongWritable, DoubleWritable, FloatWritable, TopicVocabMessage> {
                                    //vertex ID type, vertex value type, edge value type, message value type

  private static String VAR_AGG = "VarianceAggregator";
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(TopicVertex.class);

  private float TWEET_SIZE = 20000;
  private float epson = 0.01f;
  private double counter = 0;

  /*
   * Is this vertex the source id?
   *
   * @return True if the source id
  *private boolean isSource() {
  *  return getId().get() == SOURCE_ID.get(getConf());
  *}
  */
  @Override
  public void compute(Iterable<TopicVocabMessage> messages) {
	
    if(getSuperstep() == 0){  //Each vocab will broadcast its neighbor IDs
        if(getValue().equals(new DoubleWritable(0))){ //0 for vocab
			int edgesNum = getNumEdges();
			int[] neighbors = new int[edgesNum];
			neighbors[0] = edgesNum - 1;
            for (Edge<LongWritable, FloatWritable> edge : getEdges()){
				long vertexid = edge.getTargetVertexId().get();
				int index = 1;
				for (Edge<LongWritable, FloatWritable> edge2 : getEdges()) {
					long neighbourid = edge2.getTargetVertexId().get();
					if (vertexid == neighbourid)
						continue;
					neighbors[index++] = (int) neighbourid;
				}
                sendMessage(edge.getTargetVertexId(), new TopicVocabMessage((int)getId().get(), neighbors)); 
            }
         }
    } 
	
	else if(getSuperstep() == 1){
        if(getValue().equals(new DoubleWritable(1))){ //1 for tweet
		  setValue(new DoubleWritable(0.0));
		  HashMap<Integer, Float> tfidfs = new HashMap<Integer, Float> ();
          HashMap<Integer, ArrayList<Integer>> neighborTweets = new HashMap<Integer, ArrayList<Integer>>(); //The tweets that share at least one vocab with me
          float norm = 0f;
		  for (TopicVocabMessage message : messages) {
              int vocabId = message.getSourceId();
              int[] tweetId = message.getNeighborId();
			  //tweetId[0] = (int)getId().get();
			  float idf = (TWEET_SIZE) / tweetId.length;
			  LongWritable vocabLong = new LongWritable(new Long(vocabId));
              float tf = getEdgeValue(vocabLong).get();
              float tfIDF = tf * idf;
			  setEdgeValue(vocabLong, new FloatWritable(tfIDF));
			  norm += tfIDF * tfIDF;
			  for(int i = 1; i < tweetId.length; i++){
				ArrayList<Integer> neighbourTweetsVocab = neighborTweets.get(tweetId[i]);
				if (neighbourTweetsVocab == null) {
					neighbourTweetsVocab = new ArrayList<Integer> ();
					neighborTweets.put(tweetId[i], neighbourTweetsVocab);
				}
				neighbourTweetsVocab.add(vocabId);
			  }
			  tfidfs.put(vocabId, tfIDF);
          }
		  norm = (float)Math.sqrt(norm);
		  if (neighborTweets.size() != 0)
			setValue(new DoubleWritable(norm));
          //Send the TF-IDF (the feature vector) to all the tweets that share at least one vocab with the current tweet (neighborTweets)
		  for(Integer tweet : neighborTweets.keySet()){
				Long tweetid = new Long(tweet);
				ArrayList<Integer> edgesNum = neighborTweets.get(tweet);
				float[] mytfidf = new float[edgesNum.size()+2];
				int[] neighbors = new int[edgesNum.size()+1];
				mytfidf[0] = edgesNum.size()+1;
				neighbors[0] = edgesNum.size();
				int index = 1;
				for(Integer vocabid : edgesNum){
					mytfidf[index] = tfidfs.get(vocabid);
					neighbors[index++] = vocabid;  
				}		 
				mytfidf[index] = norm;				
				sendMessage(new LongWritable(tweetid), new TopicVocabMessage((int)getId().get(), neighbors, mytfidf));  
                //message content: currentTweet, vocab ID, TF-IDF. Sent to tweet
          }
        }
		
    } else if(getSuperstep() == 2){
      //if(getValue().equals(new DoubleWritable(1))){ //1 for tweet
        //Calculate similarity with other tweets that share at least one vocab with the current tweet
        ArrayList<Float> sims = new ArrayList<Float> ();
        //Calculate the norm of the current tweet
        float norm = new Float(getValue().get());
		for (TopicVocabMessage message : messages) {
		      float cosSim = 0.0f;
              int tweet = message.getSourceId();
              int[] vocabId = message.getNeighborId();
              float[] tfidf = message.getTFIDF();
              HashMap<Integer, Float> currentTweetTFIDF =  new HashMap<Integer, Float> ();
			  float norm2 = tfidf[tfidf.length-1];
			  int size = vocabId[0];
			  for(int i = 1; i <= size; i++) {
				currentTweetTFIDF.put(vocabId[i], tfidf[i]);
		      }
			  for(Edge<LongWritable, FloatWritable> edge : getEdges()){
                if(currentTweetTFIDF.containsKey((int)edge.getTargetVertexId().get())){
                  cosSim += edge.getValue().get() * currentTweetTFIDF.get((int)edge.getTargetVertexId().get());
                }
             }
			 float similarity = cosSim/(norm * norm2);
			 sims.add(similarity);
			 if(similarity >= epson) { // add edge between the two tweets
				addEdge(EdgeFactory.create(new LongWritable(tweet), new FloatWritable(similarity)));;
			 }
		 }
          //Calculate the variance
          float var = getVariance(sims);
          setValue(new DoubleWritable(var));
		  
      //}
    }
	

    voteToHalt();
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


