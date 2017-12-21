package freebase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.collections4.CollectionUtils;


//returns the embeddings for the top 4000 most common english words
public class Visualization {
	public static void getTop4k(String type, String src, String duplicate, String fixed){
		HashSet<String> vocab = new HashSet<String>(8000);
		String inputEmbeddings;
		if (src == "")
			inputEmbeddings = "/media/brian/3TB HDD/Datasets/Freebase/6_Embeddings/"+type+"/freebase-rdf-latest-embeddings"+duplicate+".txt";
		else
			inputEmbeddings = "/media/brian/3TB HDD/Models/Experiments_Final/"+type+fixed+ "/"+src+"embeddings.txt";
		String inputTop4k = "/media/brian/3TB HDD/Models/Experiments_Final/top4k.csv";
		String outputEmbeddings = "/media/brian/3TB HDD/Models/Experiments_Final/"+type+fixed+ "/"+type+"-"+src+"embeddings4k"+duplicate+".txt";
		
		System.out.println("Trimming: " + type + " to top 4k english words");
		FileReader efr = null;
		BufferedReader ebr = null;
		FileReader tfr = null;
		BufferedReader tbr = null;
		
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			tfr = new FileReader(inputTop4k);
			tbr = new BufferedReader(tfr);

			
			System.out.println("Reading in top english vocabulary");
			for (String line; (line = tbr.readLine()) != null;) {
				vocab.add(line);
			}
			tbr.close();
			
			efr = new FileReader(inputEmbeddings);
			ebr = new BufferedReader(efr);
			fw = new FileWriter(outputEmbeddings);
			bw = new BufferedWriter(fw);

			String lines[];
			System.out.println("Writing matching embeddings");
			for (String line; (line = ebr.readLine()) != null;) {
				lines = line.split(" ");
				if (vocab.contains(lines[0])){
					if(line.endsWith(" "))
						line = line.substring(0, line.length()-1);
					bw.write(line);
					bw.newLine();
				}
			}

			System.out.println("Done");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
				if (ebr != null)
					ebr.close();
				if (efr != null)
					efr.close();
				if (tbr != null)
					tbr.close();
				if (tfr != null)
					tfr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void  getOriginalWord2Vec() {
		String inputSrcEmbeddings = "/media/brian/3TB HDD/Models/Experiments_Final/Word2Vec_Fixed/src_embeddings.txt";
		String inputTgtEmbeddings = "/media/brian/3TB HDD/Models/Experiments_Final/Word2Vec_Fixed/tgt_embeddings.txt";
		String outputEmbeddings = "/media/brian/3TB HDD/Datasets/Freebase/6_Embeddings/Word2Vec_Fixed/freebase-rdf-latest-embeddings.txt";
		
		HashMap<String, String> entityToLine = new HashMap<String, String>();
		
		System.out.println("Creating original Word2Vec embeddings from fixed src - fixed tgt");
		FileReader sfr = null;
		BufferedReader sbr = null;
		FileReader tfr = null;
		BufferedReader tbr = null;
		
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			sfr = new FileReader(inputSrcEmbeddings);
			sbr = new BufferedReader(sfr);
			tfr = new FileReader(inputTgtEmbeddings);
			tbr = new BufferedReader(tfr);
			
			String lines[];
			System.out.println("Reading src embeddings");
			for (String line; (line = sbr.readLine()) != null;) {
				lines = line.split(" ");
				entityToLine.put(lines[0], line);
			}
			
			System.out.println("Reading tgt embeddings");
			for (String line; (line = tbr.readLine()) != null;) {
				lines = line.split(" ");
				
				if (entityToLine.containsKey(lines[0]) && entityToLine.get(lines[0]) != line) {
					System.out.println(entityToLine.get(lines[0]));
					System.out.println(line);
					System.out.println("");
					
					entityToLine.remove(lines[0]);
				}
			}
			
			fw = new FileWriter(outputEmbeddings);
			bw = new BufferedWriter(fw);
			
			System.out.println("Writing embeddings");
			for (String entity : entityToLine.keySet()) {
				bw.write(entityToLine.get(entity));
				bw.newLine();
			}

			

			System.out.println("Done");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
				if (sbr != null)
					sbr.close();
				if (sfr != null)
					sfr.close();
				if (tbr != null)
					tbr.close();
				if (tfr != null)
					tfr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void getCommonkNNDistribution(String name, int k){
		String inputBefore = "/media/brian/3TB HDD/Distribution/"+name+"_before.txt";
		String inputAfter = "/media/brian/3TB HDD/Distribution/"+name+"_after.txt";
		
		String outputDistributions = "/media/brian/3TB HDD/Distribution/"+name+"_distribution.txt";
		ArrayList<HashMap<Integer, Integer>> freqs = new ArrayList<>();
		// i nearest neighbors
		for (int i = 1; i <= k; i++){
			HashMap<Integer, Integer> freq = new HashMap<>();
			for(int j = 0; j <= i; j++){
				freq.put(j,0);
			}
			freqs.add(freq);
		}
		
		HashMap<String, double[]> before = new HashMap<>();
		HashMap<String, double[]> after = new HashMap<>();
		
		System.out.println("Getting common kNN distribution for k=1-" + k + ": " + name);
		FileReader bfr = null;
		BufferedReader bbr = null;
		FileReader afr = null;
		BufferedReader abr = null;
		
		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			bfr = new FileReader(inputBefore);
			bbr = new BufferedReader(bfr);
			afr = new FileReader(inputAfter);
			abr = new BufferedReader(afr);
			
			String lines[];
			//System.out.println("Reading after embeddings");
			for (String line; (line = abr.readLine()) != null;) {
				lines = line.split(" ");
				double[] vector = new double[50]; 
				for(int i = 1; i < 51; i++){
					vector[i-1]=Double.parseDouble(lines[i]);
				}
				after.put(lines[0], vector);
			}
			System.out.println("after size: " + after.size());
			
			//System.out.println("Reading before embeddings");
			for (String line; (line = bbr.readLine()) != null;) {
				lines = line.split(" ");
				if (after.containsKey(lines[0])){
					double[] vector = new double[50]; 
					for(int i = 1; i < 51; i++){
						vector[i-1]=Double.parseDouble(lines[i]);
					}
					before.put(lines[0], vector);
				}
			}
			System.out.println("before size: " + before.size());
			
			System.out.print("Getting common kNN distribution");
			int count = 0;
			for (String word : before.keySet()){
			//String word = "man";
				ArrayList<String> beforekNN = kNN(word, before, k);
				
				ArrayList<String> afterkNN = kNN(word, after, k);
				//System.out.println(word + ": " + beforekNN);
				//System.out.println(word + ": " + afterkNN);
				for (int i = 1; i <= k; i++){
					int frequency = CollectionUtils.intersection(beforekNN.subList(0, i), afterkNN.subList(0, i)).size();
					freqs.get(i-1).put(frequency, freqs.get(i-1).get(frequency)+1);
				}
				if (++count%2000==0)System.out.print(".");
			}
			System.out.println(".");
			
//			for (int i = 0; i <= k; i++){
//				System.out.println(i + ", " + freq.get(i));
//			}
			
			fw = new FileWriter(outputDistributions);
			bw = new BufferedWriter(fw);
			
			//System.out.println("Writing embeddings");
			for (int i = 1; i <= k; i++){
				bw.write("Freq, Common");
				bw.newLine();
				for(int j = 0; j <= i; j++){
					bw.write(j + ", " + freqs.get(i-1).get(j));
					bw.newLine();
				}
				bw.newLine();
			}
			

			System.out.println("Done");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
//				if (bw != null)
//					bw.close();
//				if (fw != null)
//					fw.close();
				if (bbr != null)
					bbr.close();
				if (bfr != null)
					bfr.close();
				if (abr != null)
					abr.close();
				if (afr != null)
					afr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static double cosineSimilarity(double[] vectorA, double[] vectorB) {
	    double dotProduct = 0.0;
	    double normA = 0.0;
	    double normB = 0.0;
	    for (int i = 0; i < vectorA.length; i++) {
	        dotProduct += vectorA[i] * vectorB[i];
	        normA += Math.pow(vectorA[i], 2);
	        normB += Math.pow(vectorB[i], 2);
	    }   
	    return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
	}
	
	//returns word + distance hash map (unsorted)
	public static ArrayList<String> kNN(String targetName, HashMap<String, double[]> embeddings, int k){
		TreeSet<Double> set = new TreeSet<>();
		HashMap<Double, String> embeddingsFlip = new HashMap<>();
		
		double[] targetVector = embeddings.get(targetName);
		//double kNNMax;
		for (String word : embeddings.keySet()){
			if (word.equals(targetName)||word.startsWith("<"))continue;
			double distance = Math.abs(cosineSimilarity(targetVector, embeddings.get(word)));
			set.add(distance);
			embeddingsFlip.put(distance, word);
		}
		
		Iterator<Double> iterator = set.descendingIterator();
	    ArrayList<String> result = new ArrayList<>(k); //to store first K items
	    for (int i=0;i<k;i++) result.add(embeddingsFlip.get(iterator.next())); //iterator returns items in order
		    
		return result;
	}
	
	public static void main(String[] args) {
		//getTop4k("GloVe", "", "", ""); //original
		//getTop4k("GloVe", "src_", "", "");
		////getTop4k("GloVe", "", "", "_Fixed"); //fixed = original
		////getTop4k("GloVe", "src_", "", "_Fixed"); //src = fixed
		
		//getTop4k("TransE", "", "", "");
		//getTop4k("TransE", "src_", "", "");
		
		//getTop4k("TransH", "", "");
		//getTop4k("TransH", "src_", "", "");
		
		//getOriginalWord2Vec();
		//getTop4k("Word2Vec", "", "", "");
		//getTop4k("Word2Vec", "", "", "_Fixed");
		//word2vec original = word2vec_fixed_src - word2vec_fixed_tgt (remove different vectors, then get top 4k)
		
		
		//getCommonkNNDistribution("TransE", 500);
		//getCommonkNNDistribution("GloVe", 500);
		getCommonkNNDistribution("Word2Vec", 500);
		getCommonkNNDistribution("TransH", 500);
	}
}
