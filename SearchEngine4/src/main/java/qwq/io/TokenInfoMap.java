package qwq.io;


public class TokenInfoMap{
    public Long rank;
    public String offset;

    public TokenInfoMap(long rank, String offset){
        this.rank = new Long(rank);
        this.offset = new String(offset);
    }

    public TokenInfoMap add(long l, String s){
        this.rank += l;
        this.offset += (":" + s);
        return this;
    }
}
