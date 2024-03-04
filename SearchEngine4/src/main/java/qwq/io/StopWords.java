package qwq.io;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;

public class StopWords {
    public HashSet<String> set = new HashSet<String>();
    public StopWords(){
        InputStream in = this.getClass().getResourceAsStream("/stop_words.txt");
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))){
            String line=null;
			while((line = reader.readLine())!=null) {
				set.add(line.trim());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
