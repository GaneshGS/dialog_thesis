package freebase;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.json.*;
import org.jgrapht.*;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

public class Beam {
	public static String ReadFile(String path, Charset encoding) 
			  throws IOException 
			{
			  byte[] encoded = Files.readAllBytes(Paths.get(path));
			  return new String(encoded, encoding);
			}
	
	public static void FilterBeam(){
		String inputBeam = "C:/Users/Brian/Downloads/datasets/beam.json";
		String outputBeam = "C:/Users/Brian/Downloads/datasets/beam_filtered.txt";
		
		
		FileWriter fw = null;
		BufferedWriter bw = null;
		System.out.println("Filter beam");
		try {
			fw = new FileWriter(outputBeam);
			bw = new BufferedWriter(fw);
			
			String beamString = ReadFile(inputBeam, Charset.defaultCharset());

			JSONObject beamObject = new JSONObject(beamString);
			JSONArray allPredictedIDs, allScores, allParentIDs;
			allPredictedIDs = beamObject.getJSONArray("predicted_ids");
			allScores = beamObject.getJSONArray("scores");
			allParentIDs = beamObject.getJSONArray("beam_parent_ids");
			System.out.println("Questions: " + allPredictedIDs.length() + "/"+allScores.length()+"/"+allParentIDs.length());
			JSONArray predictedIDsQuestion, scoresQuestion, beamParentIDsQuestion;
			
			HashMap<NodeID, Node> nodes = new HashMap<NodeID, Node>(); 
			//separate beam by question
			ArrayList<ArrayList<String>> responses = new ArrayList<>();
			HashMap<String, Integer> responseFrequency = new HashMap<>();
			for (int i = 0; i < allPredictedIDs.length(); i++){
				Graph<Node, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);
				predictedIDsQuestion = allPredictedIDs.getJSONArray(i);
				scoresQuestion = allScores.getJSONArray(i);
				beamParentIDsQuestion = allParentIDs.getJSONArray(i);
				NodeID root = new NodeID(0,0);
				Node rootNode = new Node(root, "START", 0f);
				nodes.put(root, rootNode);
				graph.addVertex(rootNode);
				for (int j = 0; j < predictedIDsQuestion.length(); j++){
					JSONArray names = predictedIDsQuestion.getJSONArray(j);
					JSONArray scores = scoresQuestion.getJSONArray(j);
					JSONArray parentIDs = beamParentIDsQuestion.getJSONArray(j);
					for (int k = 0; k < names.length(); k++){
						//level = j+1
						NodeID id = new NodeID(j+1, k);
						double score;
						if (scores.get(k).toString().equals("null"))
							score = Double.NEGATIVE_INFINITY;
						else {
							if (scores.get(k).getClass() == Integer.class)
								score = new Double((int)scores.get(k));
							 else
								score = new Double((double)scores.get(k));
						}
						Node n = new Node(id, names.get(k).toString(), score);
						nodes.put(id,  n);
						graph.addVertex(n);
						Node parent = nodes.get(new NodeID(j, parentIDs.getInt(k)));
						graph.addEdge(parent, n);
					}
				}


		        double penaltyGamma = 0.0, penaltyBeta = 0.0, initialBeta = 0.0;
		        HashMap<String, Double> tokenPenalty = new HashMap<>();
		        Iterator<Node> iter = new BreadthFirstIterator<>(graph);
		        while (iter.hasNext()) {
		            Node vertex = iter.next();
		            ArrayList<Node> children = new ArrayList<>();
		            for(DefaultEdge e: graph.edgesOf(vertex)){
		            	children.add(graph.getEdgeTarget(e));
		            }
		            Collections.sort(children, Collections.reverseOrder());
		            for(int a = 0; a < children.size(); a++){
		            	Node child = children.get(a);
		            	if (child.penalty == 0){
		            		//METHOD 1
		            		child.penalty = vertex.penalty + penaltyGamma*a;
		            		//METHOD 2
		            		//remove penalty for punctuation
		            		if (!child.name.matches("[`~,.;:/?'\"-_]")){
			            		if (tokenPenalty.containsKey(child.name)){
			            			tokenPenalty.put(child.name, tokenPenalty.get(child.name)+1*penaltyBeta);
			            		}
			            		else {
			            				//+ "/[.,\/#!$%\^&\*;:{}=\-_`~()]/g")){
			            			tokenPenalty.put(child.name, initialBeta*penaltyBeta);
			            		}
			            		child.penalty += tokenPenalty.get(child.name);
		            		}
		            	}
		            }
		            //System.out.println(vertex.name + " is connected to: "+ graph.edgesOf(vertex).toString());
		        }
		        
		        ArrayList<ArrayList<Node>> paths = new ArrayList<>();
		        for (Node n : graph.vertexSet()){
		        	if (n.name.equals("</s>")){
		        		ArrayList<Node> path = new ArrayList<Node>();
		        		Node pathNode = n;
		        		while(!pathNode.name.equals("START")){
		        			path.add(pathNode);
		        			Set<DefaultEdge> edges = graph.edgesOf(pathNode);
		        			for (DefaultEdge edge: edges){
		        				if (graph.getEdgeSource(edge)!= pathNode){
		        					pathNode = graph.getEdgeSource(edge);
		        					break;
		        				}
		        			}
		        		}
		        		paths.add(path);
		        		//System.out.println(path);
		        	}
		        	
		        }
		        
				responses.add(new ArrayList<>());
				HashSet<String> reducedPaths = new HashSet<>();
		        //int count = 0;
		        while(!paths.isEmpty()){
		        	double minScore = Double.NEGATIVE_INFINITY;
		        	ArrayList<Node> minPath = null;
		        	ArrayList<ArrayList<Node>> badPaths = new ArrayList<ArrayList<Node>>();
		        	for (ArrayList<Node> p: paths){
		        		if (p.size() <=1){
		        			badPaths.add(p);
		        			continue;
		        		}
		        		if (p.get(1).getAdjustedScore() > minScore){
		        			minPath = p;
		        			minScore = p.get(1).getAdjustedScore();
		        		}
		        	}
		        	for (ArrayList<Node> bp : badPaths){
		        		paths.remove(bp);
		        	}
		        	//System.out.println(minScore + ": " + minPath);
		        	
		        	
		        	DecimalFormat df = new DecimalFormat("#.00");
		        	String line = "";
		        	
		        	if (minPath == null)
		        		System.out.println("error");
		        	
		        	for (int n = minPath.size() - 1; n > 0; n--){
		        		line += " " + minPath.get(n).name;
		        	}
		        	
		        	if (!reducedPaths.contains(line.replaceAll("[ .?!,]", ""))){
		        		reducedPaths.add(line.replaceAll("[ .?!,]", ""));
		        		//if (count++ < 10){
		        			//bw.write(line.trim());
		        			//bw.newLine();
		        			responses.get(i).add(line);
		        			if (responseFrequency.containsKey(line.replaceAll("[ .?!,]", ""))){
		        				responseFrequency.put(line.replaceAll("[ .?!,]", ""), responseFrequency.get(line.replaceAll("[ .?!,]", "")) + 1);
		        			} else {
		        				responseFrequency.put(line.replaceAll("[ .?!,]", ""), 1);
		        			}
		        		//}
		        		line = "[" + df.format(minScore) + "]" + line;
		        		System.out.println(line);
		        	}
		        	
		        	paths.remove(minPath);
		        }

				System.out.println("Done graph");
				//bw.newLine();
			}
			
			int count = 0;
			
			for (ArrayList<String> question: responses){
				ArrayList<String> nBestResponses = new ArrayList<>();
				for (String line: question){
					if (count < 10 && responseFrequency.get(line.replaceAll("[ .?!,]", "")) < 70 && !line.contains("<unk>")){
						count++;
						nBestResponses.add(line);
					}
				}
				
				while (!nBestResponses.isEmpty()){
					String minResponse = null;
					int minFreq = Integer.MAX_VALUE;
					for (String bestResponse: nBestResponses){
						if (responseFrequency.get(bestResponse.replaceAll("[ .?!,]", "")) < minFreq){
							minFreq = responseFrequency.get(bestResponse.replaceAll("[ .?!,]", ""));
							minResponse = bestResponse;
						}
					}
					bw.write("[" + responseFrequency.get(minResponse.replaceAll("[ .?!,]", "")) + "]" + minResponse);
					bw.newLine();
					nBestResponses.remove(minResponse);
				}
				
				count = 0;
				bw.newLine();
			}
			
			
			
			System.out.println("Done");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}  finally {
			try {
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		FilterBeam();
	}

}
