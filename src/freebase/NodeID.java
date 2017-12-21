package freebase;

public class NodeID {
	public int level, id;
	
	public NodeID(int level, int id){
		this.level = level;
		this.id = id;
	}
	
	public boolean equals (final Object O) {
	    if (!(O instanceof NodeID)) return false;
	    if (((NodeID) O).level != level) return false;
	    if (((NodeID) O).id != id) return false;
	    return true;
	  }
	
	public int hashCode() {
		  return (level << 16) + id;
	}

}
