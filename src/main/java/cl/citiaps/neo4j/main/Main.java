package cl.citiaps.neo4j.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Main {
	
		public static void main(String[] args) throws IOException {
			@SuppressWarnings("resource")
			MongoClient mongoClient = new MongoClient("localhost", 27017);
			MongoDatabase db = mongoClient.getDatabase("twitterdb");

			MongoCollection<Document> collection = db.getCollection("todo");
			
			
			Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "0995" ) );
			Session session = driver.session();

			System.out.println(session.isOpen());


			TwitterProcessor twitterProcessor = new TwitterProcessor();
			twitterProcessor.process(collection, session);

			session.close();
			driver.close();

		}
		
	

}
