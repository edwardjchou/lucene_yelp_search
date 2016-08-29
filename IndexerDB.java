package yelp.search;

import java.io.File;
import java.io.IOException;
import java.sql.* ;  // for standard JDBC programs

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;



public class IndexerDB {
	public static void main(String[] args) {
		try{ 
			IndexerDB tester = new IndexerDB();
		   Class.forName("com.mysql.jdbc.Driver").newInstance();  
		   Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/yelp_data_mining", "edwardchou", "mysqlpw");  
		   StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);  
		   
		   String indexDir;
		   
		   IndexWriterConfig configBusiness = new IndexWriterConfig(Version.LUCENE_36,
					analyzer);
		   
		   indexDir = "/Users/Edward_Chou/cs410/dbIndex_business";
		   Directory indexDirectoryBusiness = FSDirectory
					.open(new File(indexDir));
		   IndexWriter writerBusiness = new IndexWriter(indexDirectoryBusiness, configBusiness); 
		   System.out.println("Indexing to directory '" + indexDir + "'...");  
		   tester.indexDocs(writerBusiness, conn, "Business"); 
		   
		   writerBusiness.close();
		   
		   /////////////////////
		   
		   IndexWriterConfig configCategory = new IndexWriterConfig(Version.LUCENE_36,
					analyzer);
		   
		   indexDir = "/Users/Edward_Chou/cs410/dbIndex_category";
		   Directory indexDirectoryCategory = FSDirectory
					.open(new File(indexDir));
		   IndexWriter writerCategory = new IndexWriter(indexDirectoryCategory, configCategory); 
		   System.out.println("Indexing to directory '" + indexDir + "'...");  
		   tester.indexDocs(writerCategory, conn, "Category");  
		   
		   writerCategory.close();  
		   
			/////////////////////
					   
			  IndexWriterConfig configReview = new IndexWriterConfig(Version.LUCENE_36,
						analyzer);
			  
			  indexDir = "/Users/Edward_Chou/cs410/dbIndex_review";
			  Directory indexDirectoryReview = FSDirectory
						.open(new File(indexDir));
			  IndexWriter writerReview = new IndexWriter(indexDirectoryReview, configReview); 
			  System.out.println("Indexing to directory '" + indexDir + "'...");  
			  tester.indexDocs(writerReview, conn, "Review");  
			  
			  writerReview.close();  
		} 
		catch (Exception e) {  
		   e.printStackTrace();  
		}  
	}
		
	public IndexerDB(){
	}
	
	  
	void indexDocs(IndexWriter writer, Connection conn, String db_string) throws Exception { 
		/*
		business.business_id = bdata['business_id']
		        business.name = bdata['name']
		        business.full_address = bdata['full_address']
		        business.city = bdata['city']
		        business.state = bdata['state']
		        business.latitude = bdata['latitude']
		        business.longitude = bdata['longitude']
		        business.stars = decimal.Decimal(bdata.get('stars', 0))
		        business.review_count = int(bdata['review_count'])
		        business.is_open = True if bdata['open'] == "True" else False
		 */
		if(db_string == "Business"){
			  String sql = "select business_id, name, full_address, city, state, latitude, longitude, "
			  		+ "stars, review_count, is_open from Business"; 
			  System.out.println(sql);
			  
			  Statement stmt = conn.createStatement();  
			  ResultSet rs = stmt.executeQuery(sql);  
			  while (rs.next()) {  
				 //System.out.println("hi");
			     Document d = new Document();  
			     d.add(new Field("business_id", rs.getString("business_id"), Field.Store.YES, Field.Index.ANALYZED));  
			     d.add(new Field("name", rs.getString("name"), Field.Store.YES, Field.Index.ANALYZED));  
			     d.add(new Field("full_address", rs.getString("full_address"),Field.Store.YES, Field.Index.ANALYZED));
			     d.add(new Field("city", rs.getString("city"), Field.Store.YES, Field.Index.ANALYZED));  
			     d.add(new Field("state", rs.getString("state"),Field.Store.YES, Field.Index.ANALYZED));
			     d.add(new Field("latitude", rs.getString("latitude"), Field.Store.YES, Field.Index.ANALYZED));  
			     d.add(new Field("longitude", rs.getString("longitude"),Field.Store.YES, Field.Index.ANALYZED));
			     d.add(new Field("stars", rs.getString("stars"), Field.Store.YES, Field.Index.ANALYZED));  
			     d.add(new Field("review_count", rs.getString("review_count"),Field.Store.YES, Field.Index.ANALYZED));
			     d.add(new Field("is_open", rs.getString("is_open"), Field.Store.YES, Field.Index.ANALYZED));  
			     //System.out.println("Indexing " + d);
			     writer.addDocument(d);  
			 }  
			  System.out.println("done");;
		}
		/*
		 * id            
			business_id
			category_name
		 */
		else if(db_string == "Category"){
			  String sql = "select business_id, category_name from Category"; 
				  System.out.println(sql);
				  
				  Statement stmt = conn.createStatement();  
				  ResultSet rs = stmt.executeQuery(sql);  
				  while (rs.next()) {  
					 //System.out.println("hi");
				     Document d = new Document();  
				     d.add(new Field("business_id", rs.getString("business_id"), Field.Store.YES, Field.Index.ANALYZED));  
				     d.add(new Field("category_name", rs.getString("category_name"), Field.Store.YES, Field.Index.ANALYZED));  
				     //System.out.println("Indexing " + d);
				     writer.addDocument(d);  
				 }  
				  System.out.println("done");;
			}
		/*
		 * business_id
			user_id
			stars
			text
			date
			useful_votes
			funny_votes
			cool_votes
		 */
		
		else if(db_string == "Review"){
			  String sql = "select business_id, user_id, stars, text, date, useful_votes, funny_votes, cool_votes from Review"; 
				  System.out.println(sql);
				  
		
				  
				  Statement stmt = conn.createStatement();  
				  ResultSet rs = stmt.executeQuery(sql);  
				  while (rs.next()) {  
					 //System.out.println("hi");
				     Document d = new Document();  
				     d.add(new Field("business_id", rs.getString("business_id"), Field.Store.YES, Field.Index.ANALYZED));  
				     d.add(new Field("user_id", rs.getString("user_id"), Field.Store.YES, Field.Index.ANALYZED));
				     d.add(new Field("stars", rs.getString("stars"), Field.Store.YES, Field.Index.ANALYZED));
				     String fillString = rs.getString("text");
				     /*
				     if (rs.getString("text") == null){
				    	 fillString = "j3ij532ij5o32ij32"; // set it to empty string as you desire.
				    	 System.out.println("bad");
				  	}*/
				     //System.out.println(fillString);
				     d.add(new Field("text", fillString, Field.Store.YES, Field.Index.ANALYZED));  
				     d.add(new Field("date", rs.getString("date"), Field.Store.YES, Field.Index.ANALYZED));  
				     d.add(new Field("useful_votes", rs.getString("useful_votes"), Field.Store.YES, Field.Index.ANALYZED));
				     d.add(new Field("funny_votes", rs.getString("funny_votes"), Field.Store.YES, Field.Index.ANALYZED));  
				     d.add(new Field("cool_votes", rs.getString("cool_votes"), Field.Store.YES, Field.Index.ANALYZED));  
				     //System.out.println("Indexing " + d);
				     
				     writer.addDocument(d);  
				 }  
				  System.out.println("done");;
			}
	}  
}