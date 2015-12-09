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

import org.apache.giraph.aggregators.AggregateMessageCustome;
import org.apache.giraph.aggregators.VarianceAggregator;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.giraph.worker.BspServiceWorker;
import java.io.IOException;
/**
 * Demonstrates the basic Pregel shortest paths implementation.
 */
@Algorithm(
    name = "Topic Detection",
    description = "Detect Topics"
)
public class TopicVertexVocabBasedAggregateDynamic extends Vertex<LongWritable, DoubleWritable, FloatWritable, TopicVocabMessageCustome> {
                                    //vertex ID type, vertex value type, edge value type, message value type

  private static String VAR_AGG = "VarianceAggregator";
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(TopicVertexVocabBasedAggregateDynamic.class);

  private float TWEET_SIZE = 50000;
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
	    if(getSuperstep() % BspServiceWorker.MEGA_STEP == 0){  //Each vocab will broadcast its neighbor IDs
       try{
	   if (getId().get() <= 0){ //for vocab
			int edgesNum = getNumEdges();
			if(edgesNum == 0) {
				voteToHalt();
				removeVertexRequest(getId());
			}
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
			//voteToHalt();
         }
		 }catch(IOException ex){
				System.out.println(ex.getMessage());
			}
    } 
	
	
	else if(getSuperstep() % BspServiceWorker.MEGA_STEP == 1){
        if (getId().get() > 0){ // for tweet
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
		  //System.out.println(getId().get()+" : norm = "+norm);
		  int nsize = neighborTweets.size();
		  for(Integer neighbor : neighborTweets.keySet()) {
			float cosSim = 0.0f;
		    HashMap<Integer, Float> currentTweetTFIDF = neighborTweets.get(neighbor);
			for(Integer mytfidf : tfidfs.keySet()) {
					if(currentTweetTFIDF.containsKey(mytfidf)){
						cosSim += currentTweetTFIDF.get(mytfidf) * tfidfs.get(mytfidf);
					    //System.out.println(getId().get()+" , "+neighbor+" , "+mytfidf+" , "+currentTweetTFIDF.get(mytfidf)+" , "+tfidfs.get(mytfidf));
					}
			}
			cosSim = cosSim/norm;
			addEdge(EdgeFactory.create(new LongWritable(neighbor), new FloatWritable(cosSim)));
			int[] sentNeighbors = new int[1];
			float[] senttfidf = new float[1];
			senttfidf[0] = norm;
			
			sendMessage(new LongWritable(new Long(neighbor)), new TopicVocabMessageCustome((int)getId().get(),sentNeighbors, senttfidf)); 
		  }
		
		  
        }
		
    } else if(getSuperstep() % BspServiceWorker.MEGA_STEP == 2){
    		//try{
	    	    if (getId().get() > 0){
					HashMap<Integer, Float> norms = new HashMap<Integer, Float> ();
					for (TopicVocabMessageCustome message : messages) {
						 int vocabId = message.getSourceId();
						 float[] tfidfs = message.getTFIDF();
						 norms.put(vocabId, tfidfs[0]);
					}
					ArrayList<Float> sims = new ArrayList<Float> ();
					HashSet<Integer> neighbors = new HashSet<Integer>();
					for (MutableEdge<LongWritable, FloatWritable> edge : getMutableEdges()){
						int myid = (int)edge.getTargetVertexId().get();
						if(myid <= 0) // vocab vertex
							continue;
						//if(norms.get(myid) == null) {
							//System.out.println("In norm null = "+myid+", "+getId().get()+", "+getSuperstep());
						//}						
						float norm = norms.get(myid);		
						float sim = edge.getValue().get();
						sim = sim /norm;
						sims.add(sim);
						if (sim >= epson)
							neighbors.add(myid);			
						//edge.setValue(new FloatWritable(sim));
						//System.out.println(getId().get()+"\t"+myid+"\t"+sim);
					}
					 
					 //Calculate the variance
					 float var = getVariance(sims);
					 AggregateMessageCustome msg = new AggregateMessageCustome((int)getId().get(), neighbors, var);//, numMegaSetps);
					 aggregate(VarianceAggregator.VAR_AGG, msg); 
				}
				long numMegaSetps = getNumMegaSteps();
				if( numMegaSetps * BspServiceWorker.SHIFT_SIZE >= (BspServiceWorker.TOTAL_TWEETS- BspServiceWorker.WINDOW_SIZE)){
					//System.out.println("In vertex " + getId().get() +". About voting to halt, in superstep = " + getSuperstep() +", in numMegaSetps = " + numMegaSetps);
					voteToHalt();
				}
				else{
					/*
						1-Removing vertices that will chenge their identities
						2-Remove edges to the vertices that will change their identities
					*/
				  	long stRange = getStRange(numMegaSetps);
				  	long endRange = getEndRange(stRange);
				  	//System.out.println("In vertex " + getId().get() +". checking if need to load new data, in superstep = " + getSuperstep() +", in numMegaSetps = " + numMegaSetps);
					if(getId().get() >= stRange && getId().get() <= endRange){
						HashSet<LongWritable> ids = new HashSet<LongWritable> ();
						for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
						  //long targetVertexId = edge.getTargetVertexId().get();
						  ids.add(edge.getTargetVertexId());
						  //removeEdges(edge.getTargetVertexId());
				  		  //removeEdgesRequest(getId(), new LongWritable(targetVertexId));
				  		}
						//removeVertexAllEdgeRquest();
						for (LongWritable id : ids){
								removeEdges(id);
					    }
					}
					else{	//Remove edges from this vertex to the vertices that will change their identities
						if(getId().get() > 0) {
							HashSet<LongWritable> ids = new HashSet<LongWritable> ();
							for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
								long targetVertexId = edge.getTargetVertexId().get();
								if(targetVertexId > 0) {
									ids.add(edge.getTargetVertexId());
									//removeEdges(edge.getTargetVertexId());
									//removeEdgesRequest(getId(), new LongWritable(targetVertexId));
								}
							}
							for (LongWritable id : ids){
								removeEdges(id);
							}
						} else {
							HashSet<LongWritable> ids = new HashSet<LongWritable> ();
							for (Edge<LongWritable, FloatWritable> edge : getEdges()) {
								long targetVertexId = edge.getTargetVertexId().get();
								if(targetVertexId >= stRange && targetVertexId <= endRange) {
									//removeEdgesRequest(getId(), new LongWritable(targetVertexId));
								    //removeEdges(edge.getTargetVertexId());
									ids.add(edge.getTargetVertexId());
								}
							}
							for (LongWritable id : ids){
								removeEdges(id);
							}
						}
					}
				}
			//}
			//catch(IOException ex){
				//System.out.println(ex.getMessage());
			//}
		}
		else if(getSuperstep() % BspServiceWorker.MEGA_STEP == 3){
			long numMegaSetps = getNumMegaSteps() + 1;
			String hostname = getHostName();
			if((hostname != null) && (!hostname.equalsIgnoreCase(""))){
				String fileName = "/user/exp/ahmed/50k_sliding_20k_dynamic/" + numMegaSetps + "-" +  getHostName();
				setHostName("");
				System.out.println("Trying to read batch from: " + fileName);
				loadNewTimeSLot(fileName);
			}
		}	


  }

  private long getNumMegaSteps(){
  	return getSuperstep() / (BspServiceWorker.MEGA_STEP);
  }

  private long getStRange(long numMegaSetps){
  	return ( numMegaSetps * BspServiceWorker.SHIFT_SIZE ) + 1;
  }

  private long getEndRange(long stRange){
  	return stRange + BspServiceWorker.SHIFT_SIZE - 1;
  }

  private void loadNewTimeSLot(String filePath){
  try{
		//Load new data  
		Path ptRead = new Path(filePath);
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream reader = fs.open(ptRead); 
		//System.out.println("In vertex " + getId().get() +". reader = " + reader + ", in superstep = " + getSuperstep());
		String line = reader.readLine();
		if(line.length() != 0) {
			String[] splits = line.split("\t");
			for(String split : splits) {
				addVertexRequest(new LongWritable(Long.parseLong(split)) , new DoubleWritable(0)); //new vocab vertex
			}
		}
		while ((line = reader.readLine()) != null) {
			//System.out.println(line);
			String[] splits = line.split("\t");
			long tweetId = Long.parseLong(splits[0]);
			//addVertexRequest(new LongWritable(tweetId) , new DoubleWritable(0)); //new vocab vertex
			//For each tweet
			int ind = 1;
			int endInd = (splits.length-1)/2;
			for(int i = 0; i < endInd; i++){
				//create vocab vertex and add undirected edge
				long vocabId = Long.parseLong(splits[ind++]);
				float tf = Float.parseFloat(splits[ind++]);
				//LongWritable vocabIndex = new LongWritable(vocabId);
				//Add edge from the tweet to the vocab
				FloatWritable edgeVal = new FloatWritable(tf);
				//System.out.println("In vertex " + getId().get() + ", adding an edge to " + vocabId + ", superstep " + getSuperstep() + " i = " + i + ", endInd = " + endInd);
				addEdgeRequest(new LongWritable(tweetId), EdgeFactory.create(new LongWritable(vocabId) , edgeVal));
				//Add edge from the vocab to the tweet
				addEdgeRequest(new LongWritable(vocabId), EdgeFactory.create(new LongWritable(tweetId), edgeVal));
			}
		}
		reader.close();
	}
	catch(Exception e){
		System.out.println("TopicVertexVocabBasedAggregateDynamic, loading batch file, " + e.getMessage());
	}
  }

  float getMean(ArrayList<Float> data)
    {
        float sum = 0.0f;
        for(Float a : data)
            sum += a;
        return sum/(TWEET_SIZE);
    }

    float getVariance(ArrayList<Float> data)
    {
        float mean = getMean(data);
        float temp = 0f;
        for(Float a :data)
            temp += (mean-a)*(mean-a);
        temp += (mean*mean)  * (TWEET_SIZE - data.size());
        return temp/(TWEET_SIZE);
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


