package yelp.search;
import java.util.*;
import java.sql.* ; 
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.index.Term;



public class SearcherDB {
	
	IndexSearcher indexSearcher;
	QueryParser queryParser;
	Query query;
	int final_len;
	HashMap busid_hm;
	String [] text_results;
	String [] business_id_reviews;
	
	public static void main(String[] args) {
	
		try{ 
			SearcherDB tester = new SearcherDB();
			String [] bids = tester.searchDB_tester();
			/*
			for(int i = 0; i < bids.length; i++){
			
				String sql = "select * from business where bid = '" + bids[i] + "'"; 
				
				Class.forName("com.mysql.jdbc.Driver").newInstance(); 
				 Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/yelp_data_mining", "edwardchou", "mysqlpw");  
				  
				PreparedStatement pstmt = conn.prepareStatement(sql);  
				tester.displayResults(pstmt);  
			}
			*/
			
		
		}
		catch (Exception e) {  
		   e.printStackTrace();  
		}  
			
	
	}

	public SearcherDB(){
	}
	
	private String [] searchDB_tester() throws IOException, ParseException {
		
		
		SearcherDB searcher = new SearcherDB();
		String search_index; String search_word; String indexDir;
		long startTime; TopDocs hitsLoc, hitsReview, hitsFinal; long endTime;
		
		
		search_index = "city";
		search_word = "Champaign";
		indexDir = "/Users/Edward_Chou/cs410/dbIndex_business";
		startTime = System.currentTimeMillis();
		hitsLoc = searcher.searchDB(search_index, search_word, indexDir, 10000);
		endTime = System.currentTimeMillis();
		String [] business_id_locs = new String [10000];
		busid_hm = new HashMap();
		int j = 0;
		for (ScoreDoc scoreDoc : hitsLoc.scoreDocs) { 
			Document doc = searcher.getDocument(scoreDoc);
			//System.out.println(doc.get("city"));	
			//System.out.println(doc.get("name"));	
			String curr_id = doc.get("business_id");
			business_id_locs[j] = curr_id;
			busid_hm.put(curr_id, 0);
			j++;
		}

		System.out.println(hitsLoc.totalHits + " documents found. Time :"
				+ (endTime - startTime));
		   
		
		
		search_index = "text";
		search_word = "healthy";
		int res_len = 50;
		indexDir = "/Users/Edward_Chou/cs410/dbIndex_review";
		startTime = System.currentTimeMillis();
		
		hitsReview = searcher.searchDB(search_index, search_word, indexDir, 200000);
		endTime = System.currentTimeMillis();

		System.out.println(hitsReview.totalHits + " documents found. Time :"
				+ (endTime - startTime));
		
		text_results = new String [res_len];
		business_id_reviews = new String [res_len];
		int i = 0;
		String previd = "";
		for (ScoreDoc scoreDoc : hitsReview.scoreDocs) { 
			Document doc = searcher.getDocument(scoreDoc);
			//System.out.println(doc.getFields());	
			String curr_id = doc.get("business_id");
			
		
			
			
			if(doc.get("text") != null && 	
					i < res_len && 
					busid_hm.containsKey(curr_id) &&
					!curr_id.equals(previd)){
				//System.out.println(doc.get("business_id"));
				//System.out.println(doc.get("text"));
				
				//System.out.println(curr_id);
				//System.out.println(previd);
				
				
				text_results[i] = doc.get("text");
				business_id_reviews[i] = curr_id;
				
				
				
				
				i++;
			}
			if(i >= res_len){
				break;
			}
		
			final_len = i;
			
			previd = curr_id;
		}
		System.out.println(final_len);
		
		
		ServerSocket serverSocket;
		String serverName;
		int port = 7500;
		
		serverSocket = new ServerSocket(port);
		serverSocket.setSoTimeout(1000000);
		serverName = "localhost";
		/*
		
		try {
			System.out.println("Connecting to " + serverName + " on port "
					+ port);
			Socket client = new Socket(serverName, port);
			System.out.println("Just connected to "
					+ client.getRemoteSocketAddress());
			OutputStream outToServer = client.getOutputStream();
			DataOutputStream out = new DataOutputStream(outToServer);
			out.writeUTF("Hello from " + client.getLocalSocketAddress());
			InputStream inFromServer = client.getInputStream();
			DataInputStream in = new DataInputStream(inFromServer);
			System.out.println("Server says " + in.readUTF());
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		*/
		
		System.out.println("Connecting to " + serverName + " on port "
				+ port);
		
		for(i = 0; i < final_len; i++){
			search_index = "business_id";
			search_word = business_id_reviews[i];
			indexDir = "/Users/Edward_Chou/cs410/dbIndex_business";
			startTime = System.currentTimeMillis();
			hitsFinal = searcher.searchDB(search_index, search_word, indexDir, 1);
			endTime = System.currentTimeMillis();

			//System.out.println(business_id_reviews[i]);
			for (ScoreDoc scoreDoc : hitsFinal.scoreDocs) { 
				Document doc = searcher.getDocument(scoreDoc);
				if(!doc.get("name").equals("Meatheads"))
					System.out.println(doc.get("name"));	
				
			}
			
		}
		
		
		
		
		
		
		searcher.close();
		return business_id_reviews;
	}
	
	@SuppressWarnings("deprecation")
	public TopDocs searchDB(String search_index, String search_word, String indexDir, int numResults) throws IOException, ParseException{
		
		
		Directory indexDirectory = FSDirectory
				.open(new File(indexDir));
		indexSearcher = new IndexSearcher(indexDirectory);
		
		Query query = new QueryParser(
				Version.LUCENE_CURRENT, search_index, new StandardAnalyzer(Version.LUCENE_CURRENT)).parse(search_word);

		Sort sortobj = new Sort();
		Term search_term = new Term(search_word);
		PrefixFilter filt = new PrefixFilter(search_term);
		return indexSearcher.search(query, numResults, sortobj); //LuceneConstants.MAX_SEARCH);
		
	}
	public TopDocs searchDBAfter(ScoreDoc after, String search_index, String search_word, String indexDir, int numResults) throws IOException, ParseException{
		
		
		Directory indexDirectory = FSDirectory
				.open(new File(indexDir));
		indexSearcher = new IndexSearcher(indexDirectory);
		
		Query query = new QueryParser(
				Version.LUCENE_CURRENT, search_index, new StandardAnalyzer(Version.LUCENE_CURRENT)).parse(search_word);

		return indexSearcher.searchAfter(after, query, numResults); //LuceneConstants.MAX_SEARCH);
		
	}
	  
	void displayResults(PreparedStatement pstmt) {  
	   try {  
	      ResultSet rs = pstmt.executeQuery();  
	      while (rs.next()) {  
	         System.out.println(rs.getString("business_id"));  
	         System.out.println(rs.getString("city")+"\n");  
	      }  
	   } catch (SQLException e) {  
	      e.printStackTrace();  
	   }  
	}  
	public Document getDocument(ScoreDoc scoreDoc)
			throws CorruptIndexException, IOException {
		return indexSearcher.doc(scoreDoc.doc);
	}
	public void close() throws IOException {
		indexSearcher.close();
	}
}
