package org.apache.giraph.aggregators;
import java.util.HashSet;


public class Tweet implements Comparable<Tweet>{

	private float variance;
	private HashSet<Integer> conflictList;
	private int id;
	
	public Tweet(int id, float variance, HashSet<Integer> conflictList) {
		this.id = id;
		this.variance = variance;
		this.conflictList = conflictList;
	}
	
	public float getVariance() {
		return variance;
	}
	public void setVariance(float variance) {
		this.variance = variance;
	}
	public HashSet<Integer> getConflictList() {
		return conflictList;
	}
	public void setConflictList(HashSet<Integer> conflictList) {
		this.conflictList = conflictList;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}

	@Override
	public int compareTo(Tweet o) {
		if ((this.variance == o.variance) && (this.id > o.id))
			return 1;
		if ((this.variance == o.variance) && (this.id < o.id))
			return -1;
		if (this.variance > o.variance)
			return -1;
		return 1;
	}
	
}
