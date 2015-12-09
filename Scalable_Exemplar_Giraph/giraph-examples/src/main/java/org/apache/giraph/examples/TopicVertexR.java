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
import org.apache.giraph.worker.WorkerContext;
/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
    name = "Topic Detection",
    description = "Detect Topics"
)
public class TopicVertexR extends Vertex<LongWritable, DoubleWritable, FloatWritable, TopicVocabMessage> {
                                    //vertex ID type, vertex value type, edge value type, message value type

  public static String VAR_AGG = "VarianceAggregator";
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(TopicVertexR.class);

  private float TWEET_SIZE = 20000;
  private float epson = 0.01f;

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
          HashMap<Integer, int[]> vocabNeighbors = new HashMap<Integer, int[]>();
          HashSet<Long> neighborTweets = new HashSet<Long>(); //The tweets that share at least one vocab with me
          for (TopicVocabMessage message : messages) {
              int vocabId = message.getSourceId();
              int[] tweetId = message.getNeighborId();
	      tweetId[0] = (int)getId().get();
	      vocabNeighbors.put(vocabId, tweetId);
	      for(int i = 0; i < tweetId.length; i++)
		   neighborTweets.add(new Long(tweetId[i]));
          }
          for (MutableEdge<LongWritable, FloatWritable> edge : getMutableEdges()) {
              float idf = (TWEET_SIZE) / vocabNeighbors.get((int)edge.getTargetVertexId().get()).length;
              float tf = edge.getValue().get();
              float tfIDF = tf * idf;
              edge.setValue(new FloatWritable(tfIDF));
          }
          //Send the TF-IDF (the feature vector) to all the tweets that share at least one vocab with the current tweet (neighborTweets)
	  int edgesNum = getNumEdges();
          float[] mytfidf = new float[edgesNum + 1];
		  int[] neighbors = new int[edgesNum + 1];
		  mytfidf[0] = edgesNum;
		  neighbors[0] = edgesNum;
		  int index = 1;
		  for(Edge<LongWritable, FloatWritable> edge : getEdges()){
			  mytfidf[index] = edge.getValue().get();
			  neighbors[index++] = (int) edge.getTargetVertexId().get();  
		  }
		  for(Long tweet : neighborTweets){
                       if(tweet.equals(getId().get()))
                                 continue;
                        sendMessage(new LongWritable(tweet), new TopicVocabMessage((int)getId().get(), neighbors, mytfidf));  
                        //message content: currentTweet, vocab ID, TF-IDF. Sent to tweet
          }
        }
		setValue(new DoubleWritable(0.0));
    } else if(getSuperstep() == 2){
      //if(getValue().equals(new DoubleWritable(1))){ //1 for tweet
        HashMap<Integer, HashMap<Integer, Float>> neighborsTFIDF = new HashMap<Integer, HashMap<Integer, Float>> ();
        for (TopicVocabMessage message : messages) {
              int tweet = message.getSourceId();
              int[] vocabId = message.getNeighborId();
              float[] tfidf = message.getTFIDF();
              HashMap<Integer, Float> currentTweetTFIDF =  new HashMap<Integer, Float> ();
              neighborsTFIDF.put(tweet, currentTweetTFIDF);  
	      int size = vocabId[0];
	      for(int i = 1; i <= size; i++)
		currentTweetTFIDF.put(vocabId[i], tfidf[i]);
          }
          //Calculate similarity with other tweets that share at least one vocab with the current tweet
          float [] sims = new float [neighborsTFIDF.size()];
          int simInd = 0; //Index of the simialrities array
          //Calculate the norm of the current tweet
          float norm = 0f;
          for(Edge<LongWritable, FloatWritable> edge : getEdges()){
              norm += edge.getValue().get() * edge.getValue().get();
          }
          norm = (float)Math.sqrt(norm);
          //For each 2-hop neighboor (tweet not vocab)
          for(Integer neighbor: neighborsTFIDF.keySet()){
              float cosSim = 0.0f;
              //Calc the norm of the second tweet
              float norm2 = 0f;
              HashMap<Integer, Float> currentMap = neighborsTFIDF.get(neighbor);
              for (Integer vocabId : currentMap.keySet()){
                norm2 += currentMap.get(vocabId) * currentMap.get(vocabId);
              }
              norm2 = (float)Math.sqrt(norm2);
              //Calculate the numenator
              for(Edge<LongWritable, FloatWritable> edge : getEdges()){
                if(currentMap.containsKey((int)edge.getTargetVertexId().get())){
                  cosSim += edge.getValue().get() * currentMap.get((int)edge.getTargetVertexId().get());
                }
              }
              sims[simInd++] = cosSim/(norm * norm2);
	      if(sims[simInd-1] >= epson) { // add edge between the two tweets
		addEdge(EdgeFactory.create(new LongWritable(neighbor), new FloatWritable(sims[simInd-1])));;
	      }
          }
          //Calculate the variance
          float var = getVariance(sims);
          setValue(new DoubleWritable(var));
		  
      //}
    }
	voteToHalt();

  }

  float getMean(float [] data)
    {
        float sum = 0.0f;
        for(float a : data)
            sum += a;
        return sum/TWEET_SIZE;
    }

    float getVariance(float [] data)
    {
        float mean = getMean(data);
        float temp = 0f;
        for(float a :data)
            temp += (mean-a)*(mean-a);
        temp += (mean*mean)  * (TWEET_SIZE - data.length);
        return temp/TWEET_SIZE;
    }

}



