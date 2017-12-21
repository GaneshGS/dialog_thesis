package freebase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPInputStream;

public class Main {

	// Raw freebase data -> parsed triples
	public static void rawToParse(String filename, long numLines, boolean writeFile) {
		final String FB_URI = "http://rdf.freebase.com/";
		final String FB_NS_URI = "http://rdf.freebase.com/ns/";
		final String W3_URI = "http://www.w3.org/[0-9]*/[0-9]*/[0-9]*-*";

		Date currentTime = new Date();
		long startTime = System.nanoTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat("E dd/MM/yyyy  hh:mm:ss a");

		String outputFilename = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename;
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/0_Raw/" + filename + ".gz";
		InputStream fileStream = null;
		GZIPInputStream gzipStream = null;
		Reader decoder = null;
		BufferedReader br = null;
		FileWriter fw = null;
		BufferedWriter bw = null;

		System.out.println("Begin parsing raw dataset: " + filename);
		long count = 0;
		try {
			fileStream = new FileInputStream(inputFilename);
			gzipStream = new GZIPInputStream(fileStream);
			decoder = new InputStreamReader(gzipStream, "UTF-8");
			br = new BufferedReader(decoder);

			if (writeFile){
				fw = new FileWriter(outputFilename);
				bw = new BufferedWriter(fw);
			}

			String lines[];
			for (String line; (line = br.readLine()) != null;) {
				if (line.indexOf('#')>-1 && line.length() < 95)
					System.out.println(line);

				line = line.replaceAll(FB_NS_URI, "");
				line = line.replaceAll(FB_URI, "");
				line = line.replaceAll(W3_URI, "");

				lines = line.split("\\t");
				if (lines.length == 4) {
					for (int i = 0; i < 3; i++) {
						int schemaIndicatorIndex = lines[i].indexOf('#');
						if (schemaIndicatorIndex != -1) {
							// System.out.print(lines[i] + " -> ");
							if (lines[i].startsWith("<"))
								lines[i] = lines[i].substring(schemaIndicatorIndex + 1, lines[i].length() - 1);
							else
								lines[i] = lines[i].substring(schemaIndicatorIndex + 1, lines[i].length());
							// System.out.println(lines[i]);
						} else {
							if (lines[i].startsWith("<"))
								lines[i] = lines[i].substring(1, lines[i].length() - 1);
						}

					}
					line = lines[0] + "\t" + lines[1] + "\t" + lines[2];
					// System.out.println(line);
					if (writeFile){
						bw.write(line);
						bw.newLine();
					}
				}
				if (++count % 10000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + count + "/" + numLines + "| Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds));
				}
			}
			System.out.println("Total lines: " + count);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
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
				if (br != null)
					br.close();
				if (decoder != null)
					decoder.close();
				if (gzipStream != null)
					gzipStream.close();
				if (fileStream != null)
					fileStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void extractVocabulary(String filename, long numLines, boolean matchDialog) {
		Date currentTime = new Date();
		long startTime = System.nanoTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat("E dd/MM/yyyy  hh:mm:ss a");
		
		HashSet<String> dialogVocab = new HashSet<String>(80000);

		String outputFilename = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename + "-vocab";
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename;
		String inputDialogVocabulary = "/home/brian/git/OpenNMT/data/glove/opensub_qa_en.src.dict";
		System.out.println("Begin extracting vocabulary: " + filename);

		FileReader fr = null;
		BufferedReader br = null;
		FileReader dfr = null;
		BufferedReader dbr = null;

		FileWriter fw = null;
		BufferedWriter bw = null;
		long count = 0;
		long entities = 0;
		try {
			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);

			fw = new FileWriter(outputFilename);
			bw = new BufferedWriter(fw);
			String lines[];
			if (matchDialog){
				dfr = new FileReader(inputDialogVocabulary);
				dbr = new BufferedReader(dfr);
				for (String line; (line = dbr.readLine()) != null;) {
					lines = line.split(" ");
					dialogVocab.add(lines[0].toLowerCase());
				}
			}

			

			for (String line; (line = br.readLine()) != null;) {
				// System.out.println(line);

				lines = line.split("\\t");
				if (lines.length == 3) {
					if (lines[0].startsWith("m.") && lines[1].equals("type.object.name") && lines[2].endsWith("@en")) {
						// System.out.println(line);
						lines[2] = lines[2].replace("\"", "");
						lines[2] = lines[2].substring(0, lines[2].length() - 3);
						// System.out.println(lines[0] + "\t" + lines[2]);
						if (lines[2].length() > 0) {
							if (matchDialog) {
								if (dialogVocab.contains(lines[2].toLowerCase())){
									bw.write(lines[0] + "\t" + lines[2]);
									bw.newLine();
									++entities;
								}
									
							} else {
								bw.write(lines[0] + "\t" + lines[2]);
								bw.newLine();
								++entities;
							}
						}
					}
				}
				
				if (++count % 20000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + count + "/" + numLines + "| Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds)
							+ " | Entities: " + entities);
				}
			}

			System.out.println("Total lines: " + count);
			System.out.println("Total entities: " + entities);
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
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
				if (dbr != null)
					dbr.close();
				if (dfr != null)
					dfr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void findEntity(String filename, String entity) {
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename; //+ "-freqvocab";

		FileReader fr = null;
		BufferedReader br = null;

		try {
			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);
			
			String lines[];
			for (String line; (line = br.readLine()) != null;) {
				// System.out.println(line);

				lines = line.split("\\t");
//				if (lines[0].equals(entity) || lines[1].equals(entity)) {
//					System.out.println(line);
//				}
				if (lines.length == 3) {
					if (lines[0].equals(entity) && lines[1].equals("type.object.name") && lines[2].endsWith("@en")) {
						// System.out.println(line);
						lines[2] = lines[2].replace("\"", "");
						lines[2] = lines[2].substring(0, lines[2].length() - 3);
						// System.out.println(lines[0] + "\t" + lines[2]);
						if (lines[2].length() > 0) {
							System.out.println(lines[2]);
						}
					}
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void extractFreqVocabulary(String filename, long numLines) {
		Date currentTime = new Date();
		long startTime = System.nanoTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat("E dd/MM/yyyy  hh:mm:ss a");
		HashMap<String, Integer> entities = new HashMap<String, Integer>(100000000);

		String outputFilename = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename + "-freqvocab";
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename;
		System.out.println("Begin extracting frequency vocabulary: " + filename);

		FileReader fr = null;
		BufferedReader br = null;

		FileWriter fw = null;
		BufferedWriter bw = null;
		long count = 0;
		long entityCount = 0;
		try {
			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);

			fw = new FileWriter(outputFilename);
			bw = new BufferedWriter(fw);

			String lines[];
			System.out.println("Calculating entity frequencies...");
			for (String line; (line = br.readLine()) != null;) {
				lines = line.split("\t");
				if (lines.length == 3) {
					for (int i = 0; i < 3; i += 2) {
						if (lines[i].startsWith("m.")) {
							if (entities.containsKey(lines[i])) {
								entities.put(lines[i], entities.get(lines[i]) + 1);
							} else {
								entities.put(lines[i], 1);
							}
						}
					}
				}

				if (++count % 20000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + count + "/" + numLines + "| Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds)
							+ " | Entities: " + entities.size());
				}
			}

			count = 0;
			startTime = System.nanoTime();
			numLines = entities.size();
			System.out.println("Writing entities and their frequencies");
			for (String e : entities.keySet()) {
				// if (entities.get(e) > 3) {
				bw.write(e + "\t" + entities.get(e));
				bw.newLine();
				++entityCount;
				// }

				if (++count % 20000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + count + "/" + numLines + "| Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds)
							+ " | Entities: " + entityCount);
				}
			}

			System.out.println("Total lines: " + count);
			System.out.println("Total entities: " + entityCount);
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
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void sortFreqVocab(String filename, long numlines){
		HashMap<Integer, ArrayList<String>> freq2entities = new HashMap<Integer, ArrayList<String>>(100000000);

		String outputFilename = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename + "-freqvocab-sorted";
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename + "-freqvocab";
		System.out.println("Begin extracting frequency vocabulary: " + filename);

		FileReader fr = null;
		BufferedReader br = null;

		FileWriter fw = null;
		BufferedWriter bw = null;
		//long count = 0;
		//long entityCount = 0;
		try {
			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);

			fw = new FileWriter(outputFilename);
			bw = new BufferedWriter(fw);

			String lines[];
			System.out.println("Calculating entity frequencies...");
			for (String line; (line = br.readLine()) != null;) {
				lines = line.split("\t");
				if (lines.length == 2) {
					ArrayList<String> entities;
					if (!freq2entities.containsKey(Integer.parseInt(lines[1]))) {
						entities = new ArrayList<String>();
					} else {
						entities = freq2entities.get(Integer.parseInt(lines[1]));
					}
					entities.add(lines[0]);
					freq2entities.put(Integer.parseInt(lines[1]), entities);
				}
			}
			
			Object[] freqObjects= freq2entities.keySet().toArray();
			Integer[] frequencies = Arrays.copyOf(freqObjects, freqObjects.length, Integer[].class);
			Arrays.sort(frequencies, Collections.reverseOrder());
			
			for (int i:frequencies) {
				for (String entity: freq2entities.get(i)){
					bw.write(entity + "\t" + i);
					bw.newLine();
				}
			}
			
			System.out.println("Done");
			//System.out.println("Total lines: " + count);
			//System.out.println("Total entities: " + entityCount);
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
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void freqVocabularyToCSV(String filename, long numLines) {
		HashMap<Integer, ArrayList<String>> freq2entities = new HashMap<Integer, ArrayList<String>>(100000000);

		String outputFilename = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename + "-freqvocab.csv";
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename + "-freqvocab";
		System.out.println("Begin extracting frequency vocabulary: " + filename);

		FileReader fr = null;
		BufferedReader br = null;

		FileWriter fw = null;
		BufferedWriter bw = null;
		//long count = 0;
		//long entityCount = 0;
		try {
			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);

			fw = new FileWriter(outputFilename);
			bw = new BufferedWriter(fw);

			String lines[];
			System.out.println("Calculating entity frequencies...");
			for (String line; (line = br.readLine()) != null;) {
				lines = line.split("\t");
				if (lines.length == 2) {
					ArrayList<String> entities;
					if (!freq2entities.containsKey(Integer.parseInt(lines[1]))) {
						entities = new ArrayList<String>();
					} else {
						entities = freq2entities.get(Integer.parseInt(lines[1]));
					}
					entities.add(lines[0]);
					freq2entities.put(Integer.parseInt(lines[1]), entities);
				}
			}
			
			Object[] freqObjects= freq2entities.keySet().toArray();
			Integer[] frequencies = Arrays.copyOf(freqObjects, freqObjects.length, Integer[].class);
			Arrays.sort(frequencies, Collections.reverseOrder());
			
			for (int i:frequencies) {
				bw.write(i + " , " + freq2entities.get(i).size());
				bw.newLine();
			}
			

			//System.out.println("Total lines: " + count);
			//System.out.println("Total entities: " + entityCount);
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
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void parseToExtract(String filename, long numLines, int minFreq, int removeTopNFreqEntities) {
		Date currentTime = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("E dd/MM/yyyy  hh:mm:ss a");
		HashSet<String> vocab = new HashSet<String>(70000000);
		HashMap<Integer, ArrayList<String>> freq2entities;

		String outputFilename = "/media/brian/3TB HDD/Datasets/Freebase/2_Extract/" + filename;
		String outputVocabulary = "/media/brian/3TB HDD/Datasets/Freebase/2_Extract/" + filename + "-vocab";
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename;
		String inputVocabulary = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename + "-vocab";
		String inputFreqVocabulary = "/media/brian/3TB HDD/Datasets/Freebase/1_Parse/" + filename + "-freqvocab";

		FileReader fr = null;
		BufferedReader br = null;
		FileReader vfr = null;
		BufferedReader vbr = null;
		FileReader fvfr = null;
		BufferedReader fvbr = null;

		FileWriter fw = null;
		BufferedWriter bw = null;
		FileWriter vfw = null;
		BufferedWriter vbw = null;
		long count = 0;
		long triples = 0;
		try {
			vfr = new FileReader(inputVocabulary);
			vbr = new BufferedReader(vfr);
			

			String lines[];
			System.out.println("Reading in english vocabulary: " + filename + "-vocab");
			for (String line; (line = vbr.readLine()) != null;) {
				lines = line.split("\\t");
				if (lines.length == 2) {
					vocab.add(lines[0]);
				}
			}
			System.out.println("Vocabulary size: " + vocab.size());
			
			if (minFreq > 1 || removeTopNFreqEntities > 0) {
				fvfr = new FileReader(inputFreqVocabulary);
				fvbr = new BufferedReader(fvfr);
				freq2entities = new HashMap<Integer, ArrayList<String>>(100000000);
				System.out.println("Calculating entity frequencies...");
				for (String line; (line = fvbr.readLine()) != null;) {
					lines = line.split("\t");
					if (lines.length == 2) {
						ArrayList<String> entities;
						if (!freq2entities.containsKey(Integer.parseInt(lines[1]))) {
							entities = new ArrayList<String>();
						} else {
							entities = freq2entities.get(Integer.parseInt(lines[1]));
						}
						entities.add(lines[0]);
						freq2entities.put(Integer.parseInt(lines[1]), entities);
					}
				}
				
				//delete from vocab entities in the list from keys < minfreq
				for (int i = 1; i < minFreq; i++) {
					ArrayList<String> toDelete = freq2entities.get(i);
					for (String entity: toDelete) {
						if (vocab.contains(entity))
							vocab.remove(entity);
					}
				}
				
				//delete from vocab entities where value size < minentityperfreq
				Object[] freqObjects= freq2entities.keySet().toArray();
				Integer[] frequencies = Arrays.copyOf(freqObjects, freqObjects.length, Integer[].class);
				Arrays.sort(frequencies, Collections.reverseOrder());
				for (int i = 0; i < removeTopNFreqEntities; i++) {
					ArrayList<String> toDelete = freq2entities.get(frequencies[i]);
					for (String entity: toDelete) {
						if (vocab.contains(entity))
							vocab.remove(entity);
					}
				}
				
				System.out.println("Trimmed vocabulary size: " + vocab.size());
				vfw = new FileWriter(outputVocabulary);
				vbw = new BufferedWriter(vfw);
				for (String entity: vocab) {
					vbw.write(entity);
					vbw.newLine();
				}
			}

			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);
			fw = new FileWriter(outputFilename);
			bw = new BufferedWriter(fw);

			System.out.println("Begin extracting triples: " + filename);
			long startTime = System.nanoTime();
			for (String line; (line = br.readLine()) != null;) {
				// System.out.println(line);

				lines = line.split("\\t");
				if (lines.length == 3) {
					if (lines[0].startsWith("m.") && lines[2].startsWith("m.") && (vocab.contains(lines[0])
							|| vocab.contains(lines[2]))) {
						// System.out.println(line);
						bw.write(line);
						bw.newLine();
						++triples;
					}
				}

				if (++count % 20000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + count + "/" + numLines + "| Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds)
							+ " | Triples: " + triples);
				}
			}

			System.out.println("Total lines: " + count);
			System.out.println("Total triples: " + triples);
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
				if (vbw != null)
					vbw.close();
				if (vfw != null)
					vfw.close();
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
				if (vbr != null)
					vbr.close();
				if (vfr != null)
					vfr.close();
				if (fvbr != null)
					fvbr.close();
				if (fvfr != null)
					fvfr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static void reduceRelations(String filename, long numLines) {
		Date currentTime = new Date();
		long startTime = System.nanoTime();

		String outputFilename = "/media/brian/3TB HDD/Datasets/Freebase/3a_ReduceRelations/" + filename;
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/2_Extract/" + filename;

		SimpleDateFormat dateFormat = new SimpleDateFormat("E dd/MM/yyyy  hh:mm:ss a");
		System.out.println("Begin cutting triples: " + filename);

		FileReader fr = null;
		BufferedReader br = null;
		FileWriter fw = null;
		BufferedWriter bw = null;

		long count = 0;
		long triples = 0;
		try {
			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);
			fw = new FileWriter(outputFilename);
			bw = new BufferedWriter(fw);
			String lines[];

			for (String line; (line = br.readLine()) != null;) {
				lines = line.split("\\t");
				if (lines.length == 3) {
					if (!lines[1].startsWith("base") && !lines[1].startsWith("user")
							&& !lines[1].startsWith("freebase")) {
						if (lines[1].contains("..")) {
							// System.out.println(lines[1]);
							lines[1] = lines[1].substring(lines[1].lastIndexOf("..") + 2, lines[1].length());
							// System.out.println(lines[1]);
						}
						bw.write(lines[0] + "\t" + lines[1] + "\t" + lines[2]);
						bw.newLine();
						++triples;
					}
					// else
					// System.out.println(line);
				}

				if (++count % 10000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + (count) + "/" + numLines + " | Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds)
							+ " | Triples: " + triples);
				}
			}

			System.out.println("Total lines: " + count);
			System.out.println("Total triples: " + triples);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void extractEntitiesAndRelations(String folder, String filename, long numLines,
			boolean writeToFiles) {
		Date currentTime = new Date();
		long startTime = System.nanoTime();
		HashSet<String> entities = new HashSet<String>(100000000);
		HashSet<String> relations = new HashSet<String>(6000);
		// HashSet<String> uniqueLines = new HashSet<String>(615874013);
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/" + folder + "/" + filename;
		String outputEntitiesFilename = "/media/brian/3TB HDD/Datasets/Freebase/" + folder + "/" + filename
				+ "-entities";
		String outputRelationsFilename = "/media/brian/3TB HDD/Datasets/Freebase/" + folder + "/" + filename
				+ "-relations";

		SimpleDateFormat dateFormat = new SimpleDateFormat("E dd/MM/yyyy  hh:mm:ss a");
		System.out.println("Begin extracting entities and relations: " + filename);

		// InputStream fileStream = null;
		// GZIPInputStream gzipStream = null;
		// Reader decoder = null;
		// BufferedReader br = null;

		FileReader fr = null;
		BufferedReader br = null;
		FileWriter efw = null;
		BufferedWriter ebw = null;
		FileWriter rfw = null;
		BufferedWriter rbw = null;

		long count = 0;
		try {
			// fileStream = new FileInputStream(inputFilename);
			// gzipStream = new GZIPInputStream(fileStream);
			// decoder = new InputStreamReader(gzipStream, "UTF-8");
			// br = new BufferedReader(decoder);

			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);
			String lines[];

			for (String line; (line = br.readLine()) != null;) {
				// uniqueLines.add(line);
				lines = line.split("\\t");
				if (lines.length >= 3) {
					entities.add(lines[0]);
					relations.add(lines[1]);
					entities.add(lines[2]);
				} else if (lines.length == 2) {
					entities.add(lines[0]);
					System.out.println(line);
				} else {
					System.out.println(line);
				}

				if (++count % 10000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + count + "/" + numLines + " | Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds)
							+ " | Entities: " + entities.size() + " | Relations: " + relations.size());
					// " | Unique Lines: " + uniqueLines.size());
				}
			}

			if (writeToFiles) {
				efw = new FileWriter(outputEntitiesFilename);
				ebw = new BufferedWriter(efw);

				System.out.println("Writing entities: " + outputEntitiesFilename);
				// long entitiesCount = 0;
				for (String line : entities) {
					// ebw.write(line + "\t" + entitiesCount++);
					ebw.write(line);
					ebw.newLine();
				}

				rfw = new FileWriter(outputRelationsFilename);
				rbw = new BufferedWriter(rfw);
				System.out.println("Writing relations: " + outputRelationsFilename);
				// long relationsCount = 0;
				for (String line : relations) {
					// rbw.write(line + "\t" + relationsCount++);
					rbw.write(line);
					rbw.newLine();
				}
			}

			// System.out.println("Writing unique lines...");
			// for (String line : uniqueLines) {
			// ebw.write(line);
			// ebw.newLine();
			// }

			System.out.println("Total lines: " + count);
			// System.out.println("Total unique lines: " + uniqueLines);
			System.out.println("Total entities: " + entities.size());
			System.out.println("Total relations: " + relations.size());
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
				if (ebw != null)
					ebw.close();
				if (efw != null)
					efw.close();
				if (rbw != null)
					rbw.close();
				if (rfw != null)
					rfw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void reduceEntities(String filename, long numLines) {
		Date currentTime = new Date();
		long startTime = System.nanoTime();

		HashSet<String> leftEntities = new HashSet<String>(100000000);
		HashSet<String> rightEntities = new HashSet<String>(100000000);

		String outputFilename = "/media/brian/3TB HDD/Datasets/Freebase/3b_ReduceEntities/" + filename;
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/3a_ReduceRelations/" + filename;

		SimpleDateFormat dateFormat = new SimpleDateFormat("E dd/MM/yyyy  hh:mm:ss a");
		System.out.println("Begin cutting triples: " + filename);

		FileReader fr = null;
		BufferedReader br = null;
		FileWriter fw = null;
		BufferedWriter bw = null;

		long count = 0;
		long triples = 0;
		try {
			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);
			String lines[];

			System.out.println("Reading entities: " + filename);
			for (String line; (line = br.readLine()) != null;) {
				lines = line.split("\\t");
				if (lines.length == 3) {
					leftEntities.add(lines[0]);
					rightEntities.add(lines[2]);
				}

				if (++count % 10000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out
							.println(dateFormat.format(currentTime) + " | Elapsed: "
									+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
									+ " | Lines: " + (count) + "/" + numLines + " | Remaining: "
									+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes,
											estimatedSeconds)
									+ " | Left entities: " + leftEntities.size() + " | Right entities: "
									+ rightEntities.size());
				}
			}

			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);
			fw = new FileWriter(outputFilename);
			bw = new BufferedWriter(fw);

			count = 0;
			startTime = System.nanoTime();
			System.out.println("Reducing entities: " + filename);
			for (String line; (line = br.readLine()) != null;) {
				lines = line.split("\\t");
				if (lines.length == 3) {
					if (leftEntities.contains(lines[2]) && rightEntities.contains(lines[0])) {
						bw.write(line);
						bw.newLine();
						++triples;
					}
				}

				if (++count % 10000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + (count) + "/" + numLines + " | Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds)
							+ " | Triples: " + triples);
				}
			}

			System.out.println("Left set size: " + leftEntities.size());
			System.out.println("Right set size " + rightEntities.size());
			System.out.println("Total lines: " + count);
			System.out.println("Total triples: " + triples);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void splitIntoTrainHoldout(String filename, long numLines) {
		Date currentTime = new Date();
		long startTime = System.nanoTime();
		ArrayList<String> entities = new ArrayList<String>(100000000);
		HashSet<String> ookbEntities = new HashSet<String>(20000000);

		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/3b_ReduceEntities/" + filename;
		String inputEntitiesFilename = "/media/brian/3TB HDD/Datasets/Freebase/3b_ReduceEntities/" + filename
				+ "-entities";
		String outputTrainSet = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/" + filename + "-train";
		String outputHoldoutTriples = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/" + filename + "-holdoutTriples";
		String outputHoldoutEntities = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/" + filename
				+ "-holdoutEntities";

		SimpleDateFormat dateFormat = new SimpleDateFormat("E dd/MM/yyyy  hh:mm:ss a");
		System.out.println("Begin splitting dataset: " + filename);

		FileReader fr = null;
		BufferedReader br = null;
		FileReader efr = null;
		BufferedReader ebr = null;
		FileWriter tfw = null;
		BufferedWriter tbw = null;
		FileWriter hfw = null;
		BufferedWriter hbw = null;
		FileWriter hefw = null;
		BufferedWriter hebw = null;

		long count = 0;
		long trainingTriples = 0;
		long holdoutTriples = 0;
		try {
			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);
			efr = new FileReader(inputEntitiesFilename);
			ebr = new BufferedReader(efr);

			tfw = new FileWriter(outputTrainSet);
			tbw = new BufferedWriter(tfw);
			hfw = new FileWriter(outputHoldoutTriples);
			hbw = new BufferedWriter(hfw);
			hefw = new FileWriter(outputHoldoutEntities);
			hebw = new BufferedWriter(hefw);

			System.out.println("Reading in entities...");
			for (String entity; (entity = ebr.readLine()) != null;) {
				entities.add(entity);

			}

			// select a subset to label "out-of-kb"
			System.out.println("Selecting out-of-kb entities randomly...");
			Random r = new Random();
			while (ookbEntities.size() < entities.size() * 0.15) {
				String entity = entities.get(r.nextInt(entities.size()));
				ookbEntities.add(entity);
				hebw.write(entity);
				hebw.newLine();
			}
			System.out.println("Out-of-kb entities: " + ookbEntities.size());

			String lines[];
			System.out.println("Creating training and holdout sets...");
			for (String line; (line = br.readLine()) != null;) {
				lines = line.split("\\t");
				if (lines.length == 3) {
					if (ookbEntities.contains(lines[0]) || ookbEntities.contains(lines[2]) || r.nextInt(25) == 0) {
						++holdoutTriples;
						hbw.write(line);
						hbw.newLine();
					} else {
						++trainingTriples;
						tbw.write(line);
						tbw.newLine();
					}
				}

				if (++count % 10000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + count + "/" + numLines + " | Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds)
							+ " | Held-out triples: " + holdoutTriples);
				}
			}

			System.out.println("Total lines: " + count);
			System.out.println("Total training triples: " + trainingTriples);
			System.out.println("Total held-out triples: " + holdoutTriples);
			System.out.println("Total held-out entities: " + ookbEntities.size());

		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
				if (ebr != null)
					ebr.close();
				if (efr != null)
					efr.close();
				if (hbw != null)
					hbw.close();
				if (hfw != null)
					hfw.close();
				if (hebw != null)
					hebw.close();
				if (hefw != null)
					hefw.close();
				if (tbw != null)
					tbw.close();
				if (tfw != null)
					tfw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void splitIntoValidTest(String filename, long numLines) {
		Date currentTime = new Date();
		long startTime = System.nanoTime();
		HashSet<String> ookbEntities = new HashSet<String>(20000000);

		String inputHoldoutFilename = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/" + filename + "-holdoutTriples";
		String inputEntitiesFilename = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/" + filename
				+ "-holdoutEntities";
		String outputValidationSet = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/" + filename + "-valid";
		String outputTestSet = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/" + filename + "-test";

		SimpleDateFormat dateFormat = new SimpleDateFormat("E dd/MM/yyyy  hh:mm:ss a");
		System.out.println("Begin splitting dataset: " + filename);

		FileReader fr = null;
		BufferedReader br = null;
		FileReader efr = null;
		BufferedReader ebr = null;
		FileWriter vfw = null;
		BufferedWriter vbw = null;
		FileWriter tfw = null;
		BufferedWriter tbw = null;

		long count = 0;
		// types of entity relationships
		long tee = 0, twe = 0, tew = 0, tww = 0, vee = 0, vwe = 0, vew = 0, vww = 0;
		try {
			fr = new FileReader(inputHoldoutFilename);
			br = new BufferedReader(fr);
			efr = new FileReader(inputEntitiesFilename);
			ebr = new BufferedReader(efr);

			vfw = new FileWriter(outputValidationSet);
			vbw = new BufferedWriter(vfw);
			tfw = new FileWriter(outputTestSet);
			tbw = new BufferedWriter(tfw);

			System.out.println("Reading in out-of-kb entities...");
			for (String entity; (entity = ebr.readLine()) != null;) {
				ookbEntities.add(entity);
			}

			String lines[];
			Random r = new Random();
			int type;
			System.out.println("Creating validation and test sets...");
			for (String line; (line = br.readLine()) != null;) {
				lines = line.split("\\t");
				if (lines.length == 3) {
					if (ookbEntities.contains(lines[0])) {
						// w - w
						if (ookbEntities.contains(lines[2])) {
							type = 1;
						}
						// w - e
						else {
							type = 2;
						}
					} else {
						// e - w
						if (ookbEntities.contains(lines[2])) {
							type = 3;
						}
						// e - e
						else {
							type = 4;
						}
					}
					if (r.nextInt(2) == 0) {
						vbw.write(line);
						vbw.newLine();
						switch (type) {
						case 1:
							++vww;
							break;
						case 2:
							++vwe;
							break;
						case 3:
							++vew;
							break;
						case 4:
							++vee;
							break;
						}
					} else {
						tbw.write(line);
						tbw.newLine();
						switch (type) {
						case 1:
							++tww;
							break;
						case 2:
							++twe;
							break;
						case 3:
							++tew;
							break;
						case 4:
							++tee;
							break;
						}
					}
				}

				if (++count % 10000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + count + "/" + numLines + " | Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds));
				}
			}

			System.out.println("Total lines: " + count);
			System.out.println("Validation:");
			System.out.println("e-e: " + vee);
			System.out.println("w-e: " + vwe);
			System.out.println("e-w: " + vew);
			System.out.println("w-w: " + vww);
			System.out.println("Testing:");
			System.out.println("e-e: " + tee);
			System.out.println("w-e: " + twe);
			System.out.println("e-w: " + tew);
			System.out.println("w-w: " + tww);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
				if (ebr != null)
					ebr.close();
				if (efr != null)
					efr.close();
				if (vbw != null)
					vbw.close();
				if (vfw != null)
					vfw.close();
				if (tbw != null)
					tbw.close();
				if (tfw != null)
					tfw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void convertToKB2E(String filename, long numLines) {
		Date currentTime = new Date();
		long startTime = System.nanoTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat("E dd/MM/yyyy  hh:mm:ss a");

		HashMap<String, Integer> entities = new HashMap<String, Integer>(100000000);
		HashMap<String, Integer> relations = new HashMap<String, Integer>(6000);

		String outputFilename = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/triple2id.txt";
		String outputEntities = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/entity2id.txt";
		String outputRelations = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/relation2id.txt";
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/3b_ReduceEntities/" + filename;
		String inputEntities = "/media/brian/3TB HDD/Datasets/Freebase/3b_ReduceEntities/" + filename + "-entities";
		String inputRelations = "/media/brian/3TB HDD/Datasets/Freebase/3b_ReduceEntities/" + filename + "-relations";
		System.out.println("Begin converting format: " + filename);

		FileReader fr = null;
		BufferedReader br = null;

		FileWriter fw = null;
		BufferedWriter bw = null;

		int count = 0;
		long triples = 0;
		try {
			fr = new FileReader(inputEntities);
			br = new BufferedReader(fr);

			while (br.readLine() != null)
				count++;
			br.close();
			fr.close();

			fr = new FileReader(inputEntities);
			br = new BufferedReader(fr);
			fw = new FileWriter(outputEntities);
			bw = new BufferedWriter(fw);
			bw.write("" + count);
			bw.newLine();
			count = 0;
			for (String line; (line = br.readLine()) != null;) {
				entities.put(line, count);
				bw.write(line + "\t" + count);
				bw.newLine();
				++count;
			}
			br.close();
			fr.close();
			bw.close();
			fw.close();

			fr = new FileReader(inputRelations);
			br = new BufferedReader(fr);
			count = 0;
			while (br.readLine() != null)
				count++;
			br.close();
			fr.close();

			fr = new FileReader(inputRelations);
			br = new BufferedReader(fr);
			fw = new FileWriter(outputRelations);
			bw = new BufferedWriter(fw);
			bw.write("" + count);
			bw.newLine();
			count = 0;
			for (String line; (line = br.readLine()) != null;) {
				relations.put(line, count);
				bw.write(line + "\t" + count);
				bw.newLine();
				++count;
			}
			br.close();
			fr.close();
			bw.close();
			fw.close();

			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);
			count = 0;
			while (br.readLine() != null)
				count++;
			br.close();
			fr.close();

			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);
			fw = new FileWriter(outputFilename);
			bw = new BufferedWriter(fw);
			bw.write("" + count);
			bw.newLine();
			count = 0;
			String lines[];
			for (String line; (line = br.readLine()) != null;) {
				// System.out.println(line);

				lines = line.split("\\t");
				if (lines.length == 3) {
					// System.out.println(line);
					bw.write(entities.get(lines[0]) + "\t" + entities.get(lines[2]) + "\t" + relations.get(lines[1]));
					bw.newLine();
					++triples;
				}

				if (++count % 20000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + count + "/" + numLines + "| Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds)
							+ " | Triples: " + triples);
				}
			}

			System.out.println("Total lines: " + count);
			System.out.println("Total triples: " + triples);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
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
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static void convertToSME(String filename, long numLines) {
		Date currentTime = new Date();
		long startTime = System.nanoTime();
		SimpleDateFormat dateFormat = new SimpleDateFormat("E dd/MM/yyyy  hh:mm:ss a");

		String outputFilename = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/" + filename + "SME";
		String inputFilename = "/media/brian/3TB HDD/Datasets/Freebase/4_Final/" + filename;
		System.out.println("Begin converting format to SME: " + filename);

		FileReader fr = null;
		BufferedReader br = null;

		FileWriter fw = null;
		BufferedWriter bw = null;
		long count = 0;
		long triples = 0;
		try {
			fr = new FileReader(inputFilename);
			br = new BufferedReader(fr);

			fw = new FileWriter(outputFilename);
			bw = new BufferedWriter(fw);

			String lines[];

			for (String line; (line = br.readLine()) != null;) {
				// System.out.println(line);

				line = line.replace(".", "/");
				lines = line.split("\\t");
				if (lines.length == 3) {
					// System.out.println(line);
					bw.write("/" + lines[0] + "\t" + "/" + lines[1] + "\t" + "/" + lines[2]);
					bw.newLine();
					++triples;
				}

				if (++count % 20000000 == 0) {
					currentTime = new Date();
					long elapsedTime = (System.nanoTime() - startTime);

					long estimatedTime = elapsedTime / (count) * (numLines - count);
					estimatedTime = estimatedTime / 1000000000;
					long estimatedHours = estimatedTime / 3600;
					long estimatedMinutes = (estimatedTime % 3600) / 60;
					long estimatedSeconds = estimatedTime % 60;

					elapsedTime = elapsedTime / 1000000000;
					long elapsedHours = elapsedTime / 3600;
					long elapsedMinutes = (elapsedTime % 3600) / 60;
					long elapsedSeconds = elapsedTime % 60;
					System.out.println(dateFormat.format(currentTime) + " | Elapsed: "
							+ String.format("%02d:%02d:%02d", elapsedHours, elapsedMinutes, elapsedSeconds)
							+ " | Lines: " + count + "/" + numLines + "| Remaining: "
							+ String.format("%02d:%02d:%02d", estimatedHours, estimatedMinutes, estimatedSeconds)
							+ " | Triples: " + triples);
				}
			}

			System.out.println("Total lines: " + count);
			System.out.println("Total triples: " + triples);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
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
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) {
		final String gz = "freebase-rdf-latest";
		// final String gz = "freebase-rdf-2013-12-01-00-00";
		rawToParse(gz, 3130753066L, false);
		//extractVocabulary(gz, 3130753066L, false);
		//extractFreqVocabulary(gz, 3130753066L);
		//freqVocabularyToCSV(gz, 89334859);
		//sortFreqVocab(gz, 89334859);
		//parseToExtract(gz, 3130753066L, 35, 847);
		// extractEntitiesAndRelations("2_Extract", gz, 204379257, false);
		//reduceRelations(gz, 46540496);
		// extractEntitiesAndRelations("3a_ReduceRelations", gz, 315874013,
		// false);
		//reduceEntities(gz, 46540496);
		//extractEntitiesAndRelations("3b_ReduceEntities", gz, 46540496,
		//true);
		//splitIntoTrainHoldout(gz, 46540496);
		//splitIntoValidTest(gz, 46540496);
		//convertToKB2E(gz, 46540496);
		//convertToSME(gz + "-train", 46540496);
		//convertToSME(gz + "-valid", 46540496);
		//convertToSME(gz + "-test", 46540496);
		
		//findEntity(gz, "m.0kpv11"); //Musical Recording
	}

}
