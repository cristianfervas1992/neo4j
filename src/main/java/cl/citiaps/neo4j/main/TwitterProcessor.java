package cl.citiaps.neo4j.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.bson.Document;
import org.neo4j.driver.v1.Session;

import com.mongodb.client.MongoCollection;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;



public class TwitterProcessor{
	

	public void process(MongoCollection<Document> collection, Session session)/* throws IOException*/ {
		session.run("match (a)-[r]->(b) delete r");
		session.run("match (n) delete n");
		//session.run("DROP CONSTRAINT ON (u:User) ASSERT u.seguidores IS UNIQUE");
		//session.run("CREATE CONSTRAINT ON (u:User) ASSERT u.seguidores IS UNIQUE");
		//100

		for (Document doc : collection.find()/*.limit(20)*/) {
			
			//System.out.println("--------------------------------------------------");
			//System.out.println("--------------------------------------------------");
			
			
			//usuario que hizo el tweet
			Document user = (Document)doc.get("user");
			String user_name = user.getString("screen_name").toLowerCase();
			String userId = user.get("id").toString();
			String seguidores = user.get("followers_count").toString();
			//datos del tweet
			String favoritos = doc.get("favorite_count").toString();
			String n_retweet = doc.get("retweet_count").toString();
			String lang = doc.getString("lang").toLowerCase();
			String texto = doc.getString("text").toLowerCase();	
			texto = texto.replace("'"," ");
			if(lang.equals("es")){
					//creo nodo con usuario que creo el tweet
					session.run( String.format("merge (a:User {id: '%s', screen_name:'%s', seguidores:'%s'})", userId,user_name,seguidores) );			
			
					//caso que sea replica
					if(doc.get("in_reply_to_screen_name")!= null){
						//datos del que responden
						String replyToUser_name = doc.getString("in_reply_to_screen_name").toLowerCase();
						String replyToUserId = doc.getString("in_reply_to_user_id_str").toLowerCase();
						
						//creo nodo con usuario al cual respondieron
						session.run( String.format("merge (a:User {id: '%s', screen_name:'%s'})", replyToUserId,replyToUser_name) );
						//creo relación de reply entre ambos usuarios
						session.run(String.format("match (a:User) where a.id='%s' " 
								+" match (b:User) where b.id='%s' "
								+" create (a)-[:reply {text:'%s'}]->(b)", userId, replyToUserId,texto));
						//imprimo lo que guardé
						/*
						System.out.println("========RESPUESTA==========");
						System.out.println(user_name + "-RP->" +replyToUser_name);
						System.out.println(userId + "-RP->" +replyToUserId);
						System.out.println(texto);
						System.out.println("===========================");
						*/
					}
					
					//busco las menciones
					Document entidades = (Document)doc.get("entities");
					List<Document> mencionados = (ArrayList<Document>)entidades.get("user_mentions");
					//System.out.println(mencionados.size());
					//relaciones de mención
					for(Document menc : mencionados){
						String user_men = menc.getString("screen_name").toLowerCase();;
						String userId_men = menc.get("id").toString();
						//creo nodo con usuario al cual mencionaron
						session.run( String.format("merge (a:User {id: '%s', screen_name:'%s'})", userId_men,user_men) );
						//creo relación de mention entre ambos usuarios
						session.run(String.format("match (a:User) where a.id='%s' " 
								+" match (b:User) where b.id='%s' "
								+" create (a)-[:mention {text:'%s'}]->(b)", userId, userId_men,texto));
						//imprimo mencion
						/*
						System.out.println("========MENCION==========");
						System.out.println(user_name + "-MN->" +user_men);
						System.out.println(userId + "-MN->" +userId_men);
						System.out.println(texto);
						System.out.println("===========================");
						*/
					}
					//pregunto si es un retweet
					if(doc.get("retweeted_status")!=null){
						//busco usuario que creo publicación
						Document retweeted = (Document) doc.get("retweeted_status");
						Document user_retweeted = (Document) retweeted.get("user");
						String UR_name = user_retweeted.getString("screen_name").toLowerCase();
						String UR_id = user_retweeted.get("id").toString();
						//creo nodo con usuario al cual retweetearon
						session.run( String.format("merge (a:User {id: '%s', screen_name:'%s'})", UR_id,UR_name));
						//creo relación de mention entre ambos usuarios
						session.run(String.format("match (a:User) where a.id='%s' " 
								+" match (b:User) where b.id='%s' "
								+" create (a)-[:retweet {text:'%s'}]->(b)", userId, UR_id,texto));
						//imprimo rt
						/*
						System.out.println("=========RETWEET==========");
						System.out.println(user_name+ "-RT->"+UR_name);
						System.out.println(userId + "-RT->"+ UR_id);
						System.out.println(texto);
						System.out.println("===========================");
						*/
					}
					
			}
			/*
			else{
				System.out.println("No era espanol: "+texto);
			}
			*/
			//System.out.println("Agregado en git");
			//System.out.println("--------------------------------------------------");
			//System.out.println("--------------------------------------------------");
		}
		System.out.println("=====================================");
		System.out.println("Nodos y relaciones creados con exito.");
		System.out.println("=====================================");
	}


	
	
	

}
