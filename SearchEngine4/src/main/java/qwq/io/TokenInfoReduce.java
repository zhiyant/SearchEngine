package qwq.io;

public class TokenInfoReduce {
    public Double rank;
    public String other;

    public TokenInfoReduce(double rank, String offset){
        this.rank = new Double(rank);
        this.other = new String(offset);
    }

    public int compareTo(TokenInfoReduce o){
        int relation = this.rank.compareTo(o.rank);
        if (relation != 0){
            return relation;
        }
        return this.other.compareTo(o.other);
    }

    public String toString(){
        return String.valueOf(rank) + "@" + other;
    }
}
