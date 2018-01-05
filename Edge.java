public class Edge {
  private int fromTrans;
  private int toTrans;

  public Edge(int from, int to) {
    fromTrans = from;
    toTrans = to;   
  }

  public int getFrom() {
    return fromTrans;
  }

  public int getTo() {
    return toTrans;
  }

  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Edge)) {
      return false;
    }
    Edge e = (Edge) obj;
    //System.out.println("reach");
    return this.fromTrans == e.fromTrans && this.toTrans == e.toTrans;    
  }

  public int hashCode() {
    final int prime = 31;
    int hash = 17;
    hash = hash * prime + fromTrans;
    hash = hash * prime + toTrans;
    return hash;
  }
}