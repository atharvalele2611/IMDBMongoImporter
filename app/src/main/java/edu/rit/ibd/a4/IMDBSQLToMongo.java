package edu.rit.ibd.a4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;


public class IMDBSQLToMongo {

	public static void main(String[] args) throws Exception {
		final String dbURL = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String mongoDBURL = args[3];
		final String mongoDBName = args[4];
		
		
		System.out.println(new Date() + " -- Started");
		
		Connection con = DriverManager.getConnection(dbURL, user, pwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		int batchSize = 10000;
		// TODO 0: Your code here!
		
		/*
		 * 
		 * Everything in MongoDB is a document (both data and queries). To create a document, I use primarily two options but there are others
		 * 	if you ask the Internet. You can use org.bson.Document as follows:
		 * 
		 * 		Document d = new Document();
		 * 		d.append("name_of_the_field", value);
		 * 
		 * 	The type of the field will be the conversion of the Java type of the value.
		 * 
		 * 	Another option is to parse a string representing the document:
		 * 
		 * 		Document d = Document.parse("{ _id:1, name:\"Name\" }");
		 * 
		 * 	It will parse only well-formed documents. Note that the previous approach will use the Java data types as the types of the pieces of
		 * 		data to insert in MongoDB. However, the latter approach will not have that info as everything is a string; therefore, be mindful
		 * 		of these differences and use the approach it will fit better for you.
		 * 
		 * If you wish to create an embedded document, you can use the following:
		 * 
		 * 		Document outer = new Document();
		 * 		Document inner = new Document();
		 * 		outer.append("doc", inner);
		 * 
		 * To connect to a MongoDB database server, use the getClient method above. If your server is local, just provide "None" as input.
		 * 
		 * You must extract data from MySQL and load it into MongoDB. Note that, in general, the data in MongoDB is denormalized, which means that it includes
		 * 	redundancy. You must think of ways of extracting such redundant data in batches, that is, you should think of a bunch of queries that will retrieve
		 * 	the whole database in a format it will be convenient for you to load in MongoDB. Performing many small SQL queries will not work.
		 * 
		 * If you execute a SQL query that retrieves large amounts of data, all data will be retrieved at once and stored in main memory. To avoid such behavior,
		 * 	the JDBC URL will have the following parameter: 'useCursorFetch=true' (already added by the grading software). Then, you can control the number of 
		 * 	tuples that will be retrieved and stored in memory as follows:
		 * 
		 * 		PreparedStatement st = con.prepareStatement("SELECT ...");
		 * 		st.setFetchSize(batchSize);
		 * 
		 * where batchSize is the number of rows.
		 * 
		 * Null values in MySQL must be translated as documents without such fields.
		 * 
		 * Once you have composed a specific document with the data retrieved from MySQL, insert the document into the appropriate collection as follows:
		 * 
		 * 		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		 * 
		 * 		...
		 * 
		 * 		Document d = ...
		 * 
		 * 		...
		 * 
		 * 		col.insertOne(d);
		 * 
		 * You should focus first on inserting all the documents you need (movies and people). Once those documents are already present, you should deal with
		 * 	the mapping relations. To do so, MongoDB is optimized to make small updates of documents referenced by their keys (different than MySQL). As a 
		 * 	result, it is a good idea to update one document at a time as follows:
		 * 
		 * 		PreparedStatement st = con.prepareStatement("SELECT ..."); // Select from mapping table.
		 * 		st.setFetchSize(batchSize);
		 * 		ResultSet rs = st.executeQuery();
		 * 		while (rs.next()) {
		 * 			col.updateOne(Document.parse("{ _id : "+rs.get(...)+" }"), Document.parse(...));
		 * 			...
		 * 
		 * The updateOne method updates one single document based on the filter criterion established in the first document (the _id of the document to fetch
		 * 	in this case). The second document provided as input is the update operation to perform. There are several updates operations you can perform (see
		 * 	https://docs.mongodb.com/v3.6/reference/operator/update/). If you wish to update arrays, $push and $addToSet are the best options but have slightly
		 * 	different semantics. Make sure you read and understand the differences between them.
		 * 
		 * When dealing with arrays, another option instead of updating one by one is gathering all values for a specific document and perform a single update.
		 * 
		 * Note that array fields that are empty are not allowed, so you should not generate them.
		 *  
		 */
		
		db.getCollection("Movies").drop();
		db.createCollection("Movies");
		MongoCollection<Document> colMovies = db.getCollection("Movies");

		db.getCollection("MoviesDenorm").drop();
		db.createCollection("MoviesDenorm");
		MongoCollection<Document> colMoviesDenorm = db.getCollection("MoviesDenorm");
		
		
		PreparedStatement stMovie = con.prepareStatement("SELECT * FROM Movie");
		stMovie.setFetchSize(batchSize);
		ResultSet rsMovie = stMovie.executeQuery();
		System.out.println("Movie started " +new Date());
		List<Document> movieList = new ArrayList<Document>();
		List<Document> movieDenormList = new ArrayList<Document>();
		int count =0;
		while(rsMovie.next()) {
			count++;
			Document d = new Document();
			d.append("_id", rsMovie.getInt("id"));
			
			d.append("ptitle", rsMovie.getString("ptitle"));
			d.append("otitle", rsMovie.getString("otitle"));
			d.append("adult", rsMovie.getBoolean("adult"));
			if (rsMovie.getInt("year") !=0) {
				d.append("year", rsMovie.getInt("year"));
			}
			if (rsMovie.getInt("runtime") !=0 ) {
				d.append("runtime", rsMovie.getInt("runtime"));
			}
			if (rsMovie.getBigDecimal("rating") !=null) {
				d.append("rating", rsMovie.getBigDecimal("rating"));
			}
			if (rsMovie.getInt("totalvotes") != 0) {
				d.append("totalvotes", rsMovie.getInt("totalvotes"));
			}
			movieList.add(d);
			movieDenormList.add(new Document().append("_id", rsMovie.getInt("id")));
			if (count % batchSize == 0) {
				colMovies.insertMany(movieList);
				colMoviesDenorm.insertMany(movieDenormList);
				movieList = new ArrayList<>();
				movieDenormList = new ArrayList<>();
			}
		}
		colMovies.insertMany(movieList);
		colMoviesDenorm.insertMany(movieDenormList);
		movieList = new ArrayList<>();
		movieDenormList = new ArrayList<>();
		System.out.println("Movie ended " +new Date());
		rsMovie.close();
		stMovie.close();
		
		
		PreparedStatement stGenre = con.prepareStatement("SELECT * FROM moviegenre as mg join genre as g on mg.gid = g.id");
		stGenre.setFetchSize(batchSize);
		ResultSet rsGenre = stGenre.executeQuery();
		System.out.println("Genre started " +new Date());
		while(rsGenre.next()) {
			
			Document dToUpdate = new Document();
			dToUpdate.append("_id",rsGenre.getInt("mid"));
			
			Document update = new Document();
			Document dG = new Document().append("genres", rsGenre.getString("name"));
			update.append("$push",dG);
			colMovies.updateOne(dToUpdate, update);
		}
		System.out.println("Genre ended " +new Date());
		rsGenre.close();
		stGenre.close();
//		batchSize *= 10; 
//		
		db.getCollection("People").drop();
		db.createCollection("People");
		MongoCollection<Document> colPeople = db.getCollection("People");
		
		db.getCollection("PeopleDenorm").drop();
		db.createCollection("PeopleDenorm");
		MongoCollection<Document> colPeopleDenorm = db.getCollection("PeopleDenorm");
		
		PreparedStatement stPeople = con.prepareStatement("SELECT * FROM Person");
		stPeople.setFetchSize(batchSize);
		ResultSet rsPeople = stPeople.executeQuery();
		System.out.println("People started " +new Date());
		List<Document> peopleList = new ArrayList<Document>();
		List<Document> peopleDenormList = new ArrayList<Document>();
		count = 0;
		while(rsPeople.next()) {
			count++;
			Document d = new Document();
			d.append("_id", rsPeople.getInt("id"));
			d.append("name", rsPeople.getString("name"));
			if (rsPeople.getInt("byear") !=0) {
				d.append("byear", rsPeople.getInt("byear"));
			}
			if (rsPeople.getInt("dyear") !=0) {
				d.append("dyear", rsPeople.getInt("dyear"));
			}
			peopleList.add(d);
			peopleDenormList.add(new Document().append("_id", rsPeople.getInt("id")));
			if(count % batchSize == 0) {
				colPeople.insertMany(peopleList);
				colPeopleDenorm.insertMany(peopleDenormList);
				peopleList = new ArrayList<>();
				peopleDenormList = new ArrayList<>();
			}
		}
		colPeople.insertMany(peopleList);
		colPeopleDenorm.insertMany(peopleDenormList);
		peopleList = new ArrayList<>();
		peopleDenormList = new ArrayList<>();
		System.out.println("People ended " +new Date());
		rsPeople.close();
		stPeople.close();		
		
		
		
		final UpdateOptions uo = new UpdateOptions().upsert(true);
		
		
		PreparedStatement stActor = con.prepareStatement("SELECT * FROM ACTOR");
		stActor.setFetchSize(batchSize);
		ResultSet rsActor = stActor.executeQuery();
		
		
		System.out.println("Actor started "+new Date());
		while(rsActor.next()) {
			Document dToUpdateM = new Document();
			dToUpdateM.append("_id",rsActor.getInt("mid"));
			
			Document updateM = new Document();
			updateM.append("$push",new Document().append("actors", rsActor.getInt("pid")));
			
			colMoviesDenorm.updateOne(dToUpdateM, updateM,uo);
			
			Document dToUpdateP = new Document();
			dToUpdateP.append("_id",rsActor.getInt("pid"));
			
			Document updateP = new Document();
			updateP.append("$push",new Document().append("acted", rsActor.getInt("mid")));
			colPeopleDenorm.updateOne(dToUpdateP, updateP,uo);
		}
		System.out.println("Actor done "+new Date());
		rsActor.close();
		stActor.close();
		
		
		PreparedStatement stDirector = con.prepareStatement("SELECT * FROM DIRECTOR");
		stDirector.setFetchSize(batchSize);
		ResultSet rsDirector = stDirector.executeQuery();
		
		System.out.println("Director started "+new Date());
		while(rsDirector.next()) {
			Document dToUpdateM = new Document();
			dToUpdateM.append("_id",rsDirector.getInt("mid"));
			
			Document updateM = new Document();
			updateM.append("$push",new Document().append("directors", rsDirector.getInt("pid")));
			
			colMoviesDenorm.updateOne(dToUpdateM, updateM,uo);
			
			Document dToUpdateP = new Document();
			dToUpdateP.append("_id",rsDirector.getInt("pid"));
			
			Document updateP = new Document();
			updateP.append("$push",new Document().append("directed", rsDirector.getInt("mid")));
			colPeopleDenorm.updateOne(dToUpdateP, updateP,uo);
		}
		System.out.println("Director done "+new Date());
		rsDirector.close();
		stDirector.close();
		
		
		PreparedStatement stWriter = con.prepareStatement("SELECT * FROM WRITER");
		stWriter.setFetchSize(batchSize);
		ResultSet rsWriter = stWriter.executeQuery();
		
		System.out.println("writer started "+new Date());
		while(rsWriter.next()) {
			Document dToUpdateM = new Document();
			dToUpdateM.append("_id",rsWriter.getInt("mid"));
			
			Document updateM = new Document();
			updateM.append("$push",new Document().append("writers", rsWriter.getInt("pid")));
			
			colMoviesDenorm.updateOne(dToUpdateM, updateM,uo);
			
			Document dToUpdateP = new Document();
			dToUpdateP.append("_id",rsWriter.getInt("pid"));
			
			Document updateP = new Document();
			updateP.append("$push",new Document().append("written", rsWriter.getInt("mid")));
			colPeopleDenorm.updateOne(dToUpdateP, updateP,uo);
		}
		System.out.println("writer done "+new Date());
		stWriter.close();
		rsWriter.close();
		
		
		PreparedStatement stProducer = con.prepareStatement("SELECT * FROM PRODUCER");
		stProducer.setFetchSize(batchSize);
		ResultSet rsProducer = stProducer.executeQuery();
		
		System.out.println("Producer started "+new Date());
		while(rsProducer.next()) {
			Document dToUpdateM = new Document();
			dToUpdateM.append("_id",rsProducer.getInt("mid"));
			
			Document updateM = new Document();
			updateM.append("$push",new Document().append("producers", rsProducer.getInt("pid")));
			
			colMoviesDenorm.updateOne(dToUpdateM, updateM,uo);
			
			Document dToUpdateP = new Document();
			dToUpdateP.append("_id",rsProducer.getInt("pid"));
			
			Document updateP = new Document();
			updateP.append("$push",new Document().append("produced", rsProducer.getInt("mid")));
			colPeopleDenorm.updateOne(dToUpdateP, updateP,uo);
		}
		System.out.println("producer done "+new Date());
		stProducer.close();
		rsProducer.close();
		
		PreparedStatement stKF = con.prepareStatement("SELECT * FROM KNOWNFOR");
		stKF.setFetchSize(batchSize);
		ResultSet rsKF = stKF.executeQuery();
		System.out.println("knownfor started "+new Date());
		while(rsKF.next()) {
			
			Document dToUpdateP = new Document();
			dToUpdateP.append("_id",rsKF.getInt("pid"));
			
			Document updateP = new Document();
			updateP.append("$push",new Document().append("knownfor", rsKF.getInt("mid")));
			colPeopleDenorm.updateOne(dToUpdateP, updateP,uo);
		}
		System.out.println("knownfor done "+new Date());
		rsKF.close();
		stKF.close();
		client.close();
		con.close();
	}
	
	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}

}
