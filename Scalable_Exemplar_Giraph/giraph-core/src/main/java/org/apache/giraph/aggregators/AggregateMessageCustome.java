package org.apache.giraph.aggregators;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import org.apache.giraph.aggregators.Tweet;
import java.util.LinkedList;

public class AggregateMessageCustome implements Writable{
	private LinkedList<Tweet> tweetList;
	//private int megaSlotInd;
	//private int vertexId;
	//private HashSet<Integer> neighbor;
	//private float variance;

  /** Default constructor for reflection */
  public AggregateMessageCustome() {
  }

  public AggregateMessageCustome(int vertexId, HashSet<Integer> neighbor, float variance){ //, int megaSlotInd) {
	if(tweetList == null)
		this.tweetList = new LinkedList<Tweet>();
	//this.megaSlotInd = megaSlotInd;	
	Tweet tweet = new Tweet(vertexId, variance, neighbor);
	this.tweetList.add(tweet);
    //this.vertexId = vertexId;
    //this.neighbor = neighbor;
    //this.variance = variance;
  }

  public AggregateMessageCustome(LinkedList<Tweet> tweetList){
    this.tweetList = tweetList;
  }
  
  /*
  public int getMegaSlotInd(){
	 return this.megaSlotInd;
  }

  public void setMegaSlotInd(int megaSlotInd){
    this.megaSlotInd = megaSlotInd;
  }
  */
  public void setTweetList(LinkedList<Tweet> newList){
    this.tweetList = newList;
  }

  public LinkedList<Tweet> getTweetList(){
    return this.tweetList;
  }
/*
  public int getVertexId() {
    return this.vertexId;
  }

  public HashSet<Integer> getNeighbors(){
    return this.neighbor;
  }

  public float getVariance(){
    return this.variance;
  }
  */
  
  @Override
  public void write(DataOutput dataOutput) throws IOException {
	//dataOutput.writeInt(this.megaSlotInd);
	dataOutput.writeInt(this.tweetList.size());
	for(Tweet tweet: tweetList){
		HashSet<Integer> conflictList = tweet.getConflictList();
		dataOutput.writeInt(conflictList.size());
		dataOutput.writeInt(tweet.getId());
		for (Integer n :  conflictList)
			dataOutput.writeInt(n);
		dataOutput.writeFloat(tweet.getVariance());
	}
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    if(this.tweetList == null)
      this.tweetList = new LinkedList<Tweet> ();
	//this.megaSlotInd = dataInput.readInt();
    int numTweets = dataInput.readInt();
    for (int i = 0; i < numTweets; i++){
      int numCnflicts = dataInput.readInt();
      int tweetId = dataInput.readInt();
      HashSet<Integer> tempList = new HashSet<Integer>();
      for(int j = 0; j < numCnflicts; j++){
        tempList.add(dataInput.readInt());
      }
      float curVariance = dataInput.readFloat();
      Tweet curTweet = new Tweet(tweetId, curVariance, tempList);
      this.tweetList.add(curTweet);
    }
    /*
    int size = dataInput.readInt();
    this.vertexId = dataInput.readInt();
	this.neighbor = new HashSet<Integer>();
	for(int i = 0; i < size; i++)
		this.neighbor.add(dataInput.readInt());
	this.variance = dataInput.readFloat();
  */
  }
}