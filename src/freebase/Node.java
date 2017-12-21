package freebase;


public class Node implements Comparable<Node>{
	public NodeID id;
	public String name;
	private double score;
	public double penalty;
	
	public Node(NodeID id, String name, double score){
		this.id = id;
		this.name = name;
		this.score = score;
		penalty = 0.0;
	}
	
	public double getAdjustedScore(){
		return this.score - this.penalty;
	}
	
	public String toString(){return name+ ": "+(score-penalty);}

	@Override
	public int compareTo(Node o) {
		if((score - penalty)<(o.score-o.penalty))
	          return -1;
	    else if((score - penalty)>(o.score-o.penalty))
	          return 1;
	    return 0;
	}
}
