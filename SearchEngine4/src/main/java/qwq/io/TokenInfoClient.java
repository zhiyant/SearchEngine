package qwq.io;

public class TokenInfoClient {
    public Double tfidf;
    public Long line_offset;
    public String origin_filename;
    public String offsets;

    public TokenInfoClient(double tfidf, Long line_offset, String origin_filename, String offsets){
        this.tfidf = new Double(tfidf);
        this.line_offset = new Long(line_offset);
        this.origin_filename = new String(origin_filename);
        this.offsets = new String(offsets);
    }

    public int compareTo(TokenInfoClient o){
        int relation = this.tfidf.compareTo(o.tfidf);
        if (relation != 0){
            return - relation;
        }
        return - this.offsets.compareTo(o.offsets);
    }

    public void add(double tfidf, String offsets){
        this.tfidf += tfidf;
        this.offsets += (":" + offsets);
    }

    public String toString(){
        // <tfidf>@<line_offset>@<filename>@[<word_offset>;...]
        return String.format("%.6f@%d%s@%s", tfidf, line_offset, origin_filename, offsets);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TokenInfoClient) {
            TokenInfoClient o = (TokenInfoClient) obj;
            return line_offset.equals(o.line_offset) && origin_filename.equals(o.origin_filename);
        }
        return false;
    }
}
