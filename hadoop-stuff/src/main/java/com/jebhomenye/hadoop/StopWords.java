package com.jebhomenye.hadoop;

import java.io.InputStream;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

public class StopWords {
	private final Set<String> words;
	
	public StopWords(){
		words = loadStopWords();
	}
	
	public boolean isStopWord(String word){
		return words.contains(word.toLowerCase());
	}

	private Set<String> loadStopWords() {
		Scanner scanner = null;
		Set<String> words = null;
		
		try {
			InputStream in = this.getClass().getResourceAsStream("/stop_words.txt");
			scanner = new Scanner(in);
			words = new TreeSet<String>();
			while(scanner.hasNext()){
				words.add(scanner.next().toLowerCase());
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}finally{
			if(scanner != null){				
				scanner.close();
			}
		}
		return words;
	}
	
	public String toString(){
		return words.toString();
	}
}
