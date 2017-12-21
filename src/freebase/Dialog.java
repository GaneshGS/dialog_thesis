package freebase;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

public class Dialog {
	
	public static void FilterData() throws Exception{
		File in = null, src = null,tgt = null;
		  in = new File("C:/Users/Brian/Downloads/datasets/train.txt");
		  src = new File("C:/Users/Brian/Downloads/datasets/train-src.txt");
		  tgt = new File("C:/Users/Brian/Downloads/datasets/train-tgt.txt");
	      
		  if (!src.exists()) {
			  src.createNewFile();
		  }
		  if (!tgt.exists()) {
			     tgt.createNewFile();
		  }
		  String pattern= ".*[^\\x20-\\x7E].*";
		  BufferedReader br = new BufferedReader(new FileReader(in));
		  BufferedWriter bsrc = new BufferedWriter(new FileWriter(src));
		  BufferedWriter btgt = new BufferedWriter(new FileWriter(tgt));
	      try {
	    	  String lines[];
	    	  int count = 0;
	    	    for(String line; (line = br.readLine()) != null; ) {
	    	    	++count;
	    	    	
	    	        lines = line.split("\\?\\t");
	    	        if (lines[0].matches(pattern) || lines[1].matches(pattern)){
	    	        	System.out.println("Bad token: " + line);
	    	        	continue;
	    	        }
	    	        if (lines.length == 2){
	    	        	bsrc.write(lines[0]);
	    	        	bsrc.newLine();
	    	        	btgt.write(lines[1]);
	    	        	btgt.newLine();
	    	        }else{
	    	        	System.out.println("Bad amount: " + line);
	    	        }
	    	        if (count % 10000 == 0){
	    	        	System.out.println(count);
	    	        }
	    	    }
	    	    // line is not visible here.
	    	}catch (IOException ioe) {
	    		   ioe.printStackTrace();
	    	}
	    	finally
	    	{ 
	    	   try{
	    	      if(br!=null)
	    	    	  br.close();
	    	   }catch(Exception ex){
	    	       System.out.println("Error in closing the BufferedReader"+ex);
	    	    }
	    	   try{
	     	      if(bsrc!=null)
	     	    	 bsrc.close();
	     	   }catch(Exception ex){
	     	       System.out.println("Error in closing the src BufferedWriter"+ex);
	     	    }
	    	   try{
	     	      if(btgt!=null)
	     	    	 btgt.close();
	     	   }catch(Exception ex){
	     	       System.out.println("Error in closing the tgt BufferedWriter"+ex);
	     	    }
	    	}
	}

	public static void FinerFilter(String type){
		String inputSrc = "/media/brian/3TB HDD/Datasets/opensub_qa_en/v2/"+type+"-src.txt";
		String inputTgt = "/media/brian/3TB HDD/Datasets/opensub_qa_en/v2/"+type+"-tgt.txt";
		String outputSrc = "/media/brian/3TB HDD/Datasets/opensub_qa_en/v3/"+type+"-src.txt";
		String outputTgt = "/media/brian/3TB HDD/Datasets/opensub_qa_en/v3/"+type+"-tgt.txt";
		
		System.out.println("Trimming target based on threshold of 5.");
		FileReader sfr = null;
		BufferedReader sbr = null;
		FileReader tfr = null;
		BufferedReader tbr = null;
		
		FileWriter sfw = null;
		BufferedWriter sbw = null;
		FileWriter tfw = null;
		BufferedWriter tbw = null;
		try {
			sfr = new FileReader(inputSrc);
			sbr = new BufferedReader(sfr);
			tfr = new FileReader(inputTgt);
			tbr = new BufferedReader(tfr);
			sfw = new FileWriter(outputSrc);
			sbw = new BufferedWriter(sfw);
			tfw = new FileWriter(outputTgt);
			tbw = new BufferedWriter(tfw);
			
			String tokens[];
			String source;
			int count=0, total=0;
			for (String target; (target = tbr.readLine()) != null;) {
				source = sbr.readLine();
				tokens = target.split(" ");
				if (tokens.length >= 5){
					++count;
					sbw.write(source);
					sbw.newLine();
					tbw.write(target);
					tbw.newLine();
				}
				++total;
			}		

			System.out.println("Wrote " + count + "/" + total);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (sbw != null)
					sbw.close();
				if (sfw != null)
					sfw.close();
				if (tbw != null)
					tbw.close();
				if (tfw != null)
					tfw.close();
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
	
	public static void MovieFilter(String type){
		String inputSrc = "/home/brian/git/OpenNMT/data/movie/dialog/1_raw/"+type+"-src.txt";
		String inputTgt = "/home/brian/git/OpenNMT/data/movie/dialog/1_raw/"+type+"-tgt.txt";
		String outputSrc = "/home/brian/git/OpenNMT/data/movie/dialog/1_raw/"+type+"-src2.txt";
		String outputTgt = "/home/brian/git/OpenNMT/data/movie/dialog/1_raw/"+type+"-tgt2.txt";
		
		String pattern= ".*[^\\x20-\\x7E].*";
		System.out.println("Removing lines with non-visible unicode characters");
		FileReader sfr = null;
		BufferedReader sbr = null;
		FileReader tfr = null;
		BufferedReader tbr = null;
		
		FileWriter sfw = null;
		BufferedWriter sbw = null;
		FileWriter tfw = null;
		BufferedWriter tbw = null;
		try {
			sfr = new FileReader(inputSrc);
			sbr = new BufferedReader(sfr);
			tfr = new FileReader(inputTgt);
			tbr = new BufferedReader(tfr);
			sfw = new FileWriter(outputSrc);
			sbw = new BufferedWriter(sfw);
			tfw = new FileWriter(outputTgt);
			tbw = new BufferedWriter(tfw);
			
			String source;
			int count=0, total=0;
			for (String target; (target = tbr.readLine()) != null;) {
				source = sbr.readLine();
				if (!target.matches(pattern) && !source.matches(pattern)){
					++count;
					sbw.write(source);
					sbw.newLine();
					tbw.write(target);
					tbw.newLine();
				}
				++total;
			}		

			System.out.println("Wrote " + count + "/" + total);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (sbw != null)
					sbw.close();
				if (sfw != null)
					sfw.close();
				if (tbw != null)
					tbw.close();
				if (tfw != null)
					tfw.close();
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
	
	public static void TwitterFilter(){
		String input = "/home/brian/git/OpenNMT/data/twitter/dialog/1_raw/cleaned_corpus_en.txt";
		String trainSrc = "/home/brian/git/OpenNMT/data/twitter/dialog/1_raw/train-src.txt";
		String trainTgt = "/home/brian/git/OpenNMT/data/twitter/dialog/1_raw/train-tgt.txt";
		String validSrc = "/home/brian/git/OpenNMT/data/twitter/dialog/1_raw/valid-src.txt";
		String validTgt = "/home/brian/git/OpenNMT/data/twitter/dialog/1_raw/valid-tgt.txt";
		
		String pattern= ".*[^\\x20-\\x7E].*";
		System.out.println("Removing lines with non-visible unicode characters");
		FileReader fr = null;
		BufferedReader br = null;
		
		FileWriter tsfw = null;
		BufferedWriter tsbw = null;
		FileWriter ttfw = null;
		BufferedWriter ttbw = null;
		FileWriter vsfw = null;
		BufferedWriter vsbw = null;
		FileWriter vtfw = null;
		BufferedWriter vtbw = null;
		try {
			fr = new FileReader(input);
			br = new BufferedReader(fr);
			tsfw = new FileWriter(trainSrc);
			tsbw = new BufferedWriter(tsfw);
			ttfw = new FileWriter(trainTgt);
			ttbw = new BufferedWriter(ttfw);
			vsfw = new FileWriter(validSrc);
			vsbw = new BufferedWriter(vsfw);
			vtfw = new FileWriter(validTgt);
			vtbw = new BufferedWriter(vtfw);
			
			String source;
			int count=0, total=0;
			for (String target; (target = br.readLine()) != null;) {
				source = br.readLine();
				if (!target.matches(pattern) && !source.matches(pattern)){
					++count;
					if (count % 10 == 0) {
						vsbw.write(source);
						vsbw.newLine();
						vtbw.write(target);
						vtbw.newLine();
					}
					else {
						tsbw.write(source);
						tsbw.newLine();
						ttbw.write(target);
						ttbw.newLine();
					}
					
				}
				++total;
			}		

			System.out.println("Wrote " + count + "/" + total);

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (ttbw != null)
					ttbw.close();
				if (ttfw != null)
					ttfw.close();
				if (tsbw != null)
					tsbw.close();
				if (tsfw != null)
					tsfw.close();
				if (vtbw != null)
					vtbw.close();
				if (vtfw != null)
					vtfw.close();
				if (vsbw != null)
					vsbw.close();
				if (vsfw != null)
					vsfw.close();
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
		//FinerFilter("valid");
		//MovieFilter("valid");
		TwitterFilter();
	}

}
