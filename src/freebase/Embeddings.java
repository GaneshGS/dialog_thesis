package freebase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

public class Embeddings {

	public static void convertEmbeddingsToGlove(String filename, String type) {
		HashMap<String, String> id2word = new HashMap<String, String>();
		HashMap<String, String> word2id = new HashMap<String, String>();
		HashMap<String, Integer> freqvocab = new HashMap<String, Integer>();

		String inputEntities = "/media/brian/3TB HDD/Datasets/Freebase/6_Embeddings/entity2id.txt";
		String inputVectors = "/media/brian/3TB HDD/Datasets/Freebase/6_Embeddings/"+type+"/entity2vec.vec";
		String inputVocabulary = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename + "-vocab";
		String inputFreqVocabulary = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename + "-freqvocab-sorted";

		String outputVectors = "/media/brian/3TB HDD/Datasets/Freebase/6_Embeddings/"+type+"/" + filename + "-embeddings.txt";

		System.out.println("Begin converting Fast-TransX to GloVe");
		FileReader efr = null;
		BufferedReader ebr = null;
		FileReader vfr = null;
		BufferedReader vbr = null;

		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			vfr = new FileReader(inputFreqVocabulary);
			vbr = new BufferedReader(vfr);

			String lines[];
			System.out.println("Reading in freq vocabulary: " + filename + "-vocab");
			for (String line; (line = vbr.readLine()) != null;) {
				lines = line.split("\\t");
				freqvocab.put(lines[0], Integer.parseInt(lines[1]));
			}
			vbr.close();
			vfr.close();
			
			vfr = new FileReader(inputVocabulary);
			vbr = new BufferedReader(vfr);

			System.out.println("Reading in english vocabulary: " + filename + "-vocab");
			for (String line; (line = vbr.readLine()) != null;) {
				lines = line.split("\\t");
				lines[1] = lines[1].replaceAll(" ", "_").toLowerCase();
				if (word2id.containsKey(lines[1])){
					//check freq vocab, keep highest
					if (freqvocab.get(lines[0]) < freqvocab.get(word2id.get(lines[1])))
						continue;
					
				}
				word2id.put(lines[1], lines[0]);
			}
			vbr.close();
			vfr.close();
			
			for (String word : word2id.keySet()){
				id2word.put(word2id.get(word), word);
			}
			

			efr = new FileReader(inputEntities);
			ebr = new BufferedReader(efr);
			vfr = new FileReader(inputVectors);
			vbr = new BufferedReader(vfr);
			fw = new FileWriter(outputVectors);
			bw = new BufferedWriter(fw);

			int entityCount = Integer.parseInt(ebr.readLine());
			System.out.println("Entity count: " + entityCount);

			String entity[];
			String entityLine, vectorLine;
			for (int i = 0; i < entityCount; i++) {
				// TODO: add count/time estimate
				entityLine = ebr.readLine();
				entity = entityLine.split("\t");

				vectorLine = vbr.readLine();
				vectorLine = vectorLine.replaceAll("\t", " ");
				
				if (id2word.containsKey(entity[0])){
					bw.write(id2word.get(entity[0]) + " " + vectorLine.trim());
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
				if (vbr != null)
					vbr.close();
				if (vfr != null)
					vfr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void convertEmbeddingsToGloveDuplicate(String filename, String type) {
		HashMap<String, String> id2word = new HashMap<String, String>();

		String inputEntities = "/media/brian/3TB HDD/Datasets/Freebase/6_Embeddings/entity2id.txt";
		String inputVectors = "/media/brian/3TB HDD/Datasets/Freebase/6_Embeddings/"+type+"/entity2vec.vec";
		String inputVocabulary = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename + "-vocab";

		String outputVectors = "/media/brian/3TB HDD/Datasets/Freebase/6_Embeddings/"+type+"/" + filename + "-embeddings-duplicate.txt";

		System.out.println("Begin converting Fast-TransX to GloVe");
		FileReader efr = null;
		BufferedReader ebr = null;
		FileReader vfr = null;
		BufferedReader vbr = null;

		FileWriter fw = null;
		BufferedWriter bw = null;
		try {			
			vfr = new FileReader(inputVocabulary);
			vbr = new BufferedReader(vfr);

			String[] lines;
			System.out.println("Reading in english vocabulary: " + filename + "-vocab");
			for (String line; (line = vbr.readLine()) != null;) {
				lines = line.split("\\t");
				lines[1] = lines[1].replaceAll(" ", "_");
				id2word.put(lines[0], lines[1]);
			}
			vbr.close();
			vfr.close();
			

			efr = new FileReader(inputEntities);
			ebr = new BufferedReader(efr);
			vfr = new FileReader(inputVectors);
			vbr = new BufferedReader(vfr);
			fw = new FileWriter(outputVectors);
			bw = new BufferedWriter(fw);

			int entityCount = Integer.parseInt(ebr.readLine());
			System.out.println("Entity count: " + entityCount);

			String entity[];
			String entityLine, vectorLine;
			for (int i = 0; i < entityCount; i++) {
				// TODO: add count/time estimate
				entityLine = ebr.readLine();
				entity = entityLine.split("\t");

				vectorLine = vbr.readLine();
				vectorLine = vectorLine.replaceAll("\t", " ");
				
				if (id2word.containsKey(entity[0])){
					bw.write(id2word.get(entity[0]) + " " + vectorLine.trim());
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
				if (vbr != null)
					vbr.close();
				if (vfr != null)
					vfr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		final String gz = "freebase-rdf-latest";
		//convertEmbeddingsToGlove(gz, "transe");
		//convertEmbeddingsToGlove(gz, "transh");
		//convertEmbeddingsToGloveDuplicate(gz, "transe");
		//convertEmbeddingsToGloveDuplicate(gz, "transh");
	}

}
