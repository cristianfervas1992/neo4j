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
			//String texto = doc.getString("text").toLowerCase();	
			//texto = texto.replace("'"," ");
			String id_tweet = doc.getString("id_str").toLowerCase();	
			if(lang.equals("es")){
					//creo nodo con usuario que creo el tweet
					session.run( String.format("merge (a:User {id: '%s', screen_name: '%s', seguidores: '%s'})", userId,user_name,seguidores) );			
			
					//caso que sea replica
					if(doc.get("in_reply_to_screen_name")!= null){
						//datos del que responden
						String replyToUser_name = doc.getString("in_reply_to_screen_name").toLowerCase();
						String replyToUserId = doc.getString("in_reply_to_user_id_str").toLowerCase();
						
						//creo nodo con usuario al cual respondieron
						session.run( String.format("merge (a:User {id: '%s', screen_name: '%s'})", replyToUserId,replyToUser_name) );
						//creo relación de reply entre ambos usuarios
						session.run(String.format("match (a:User) where a.id='%s' " 
								+" match (b:User) where b.id='%s' "
								+" create (a)-[:reply {id:'%s'}]->(b)", userId, replyToUserId,id_tweet));
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
						String user_men = menc.getString("screen_name").toLowerCase();
						String userId_men = menc.get("id").toString();
						
						/*
						StatementResult result_segui = session.run(String.format("MATCH (a:User) where a.id='%s' return a.seguidores as men_seguidores", userId_men));
						if(result_segui.hasNext()){
							Record record_segui = result_segui.next();
							if(record_segui.get("men_seguidores").asString().equals("null")){
								System.out.println("existe pero sin seguidores");
							}
							else{
								System.out.println("Num_seguidores: "+record_segui.get("men_seguidores").asString());
							}
						}
						else{
							System.out.println("nope");
						}
						*/
						
						//creo nodo con usuario al cual mencionaron
						session.run( String.format("merge (a:User {id: '%s', screen_name: '%s'})", userId_men,user_men) );
						//creo relación de mention entre ambos usuarios
						session.run(String.format("match (a:User) where a.id='%s' " 
								+" match (b:User) where b.id='%s' "
								+" create (a)-[:mention {id:'%s'}]->(b)", userId, userId_men,id_tweet));
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
						String UR_seguidores = user_retweeted.get("followers_count").toString();
						//creo nodo con usuario al cual retweetearon
						session.run( String.format("merge (a:User {id: '%s', screen_name: '%s', seguidores: '%s'})", UR_id,UR_name,UR_seguidores));
						//creo relación de mention entre ambos usuarios
						session.run(String.format("match (a:User) where a.id='%s' " 
								+" match (b:User) where b.id='%s' "
								+" create (a)-[:retweet {id:'%s'}]->(b)", userId, UR_id,id_tweet));
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
			//System.out.println("--------------------------------------------------");
			//System.out.println("--------------------------------------------------");
		}
		//System.out.println("asdasdasdasdasdasdasdasdsadsadasdasdasdasdsadsadasd");
	}
	
	
	public void SNP(Session session, String id_autor){
		//contando número de usuarios que retweetearon al autor
		StatementResult result_retweet = session.run(String.format("MATCH (a:User) where a.id='%s' match (b:User)-[:retweet]->(a) return b.id as id", id_autor));
		int retweet = 0;
		while ( result_retweet.hasNext() )
        {
            result_retweet.next();
			retweet=retweet + 1;
        }
		System.out.println("No Usuarios "+retweet+" retweet");
		//contando número de usuarios que mencionaron al autor
		StatementResult result_mention = session.run(String.format("MATCH (a:User) where a.id='%s' match (b:User)-[:mention]->(a) return b.id as id", id_autor));
		int mention = 0;
		while ( result_mention.hasNext() )
        {
            result_mention.next();
			mention=mention + 1;
        }
		System.out.println("No Usuarios "+mention+" menciones");
		//obtengo los seguidores
		int seguidores = 0;
		StatementResult result_n_seguidores = session.run(String.format("MATCH (a:User) where a.id='%s' return a.seguidores as seguidores1", id_autor));
		
		//esto debería quitarse y ser más simple si es que no existieran nodos duplicados
		while(result_n_seguidores.hasNext()){
			Record record_segui = result_n_seguidores.next();
			if(!(record_segui.get("seguidores1").asString().equals("null"))){
				
				seguidores =  Integer.parseInt(record_segui.get("seguidores1").asString());
			}
		}
		System.out.println("No Usuarios "+seguidores+" seguidores");
		
		float Ir = 0;
		if(seguidores != 0){	
			Ir = (retweet + mention)/seguidores;
		}
		System.out.println("\nIr="+Ir);
		
		
		//nùmero de rt_tweet
		StatementResult result_tweets_rt = session.run(String.format("MATCH (a:User) where a.id='%s' match (b:User)-[r:retweet]->(a) return r.id as id_tweet_rt", id_autor));
		int tweets_rt = 0;
		List <String> id_de_tweet_rt = new ArrayList<String>();
		while (result_tweets_rt.hasNext() )
        {
			Record record_tweet_rt = result_tweets_rt.next();
			if(!id_de_tweet_rt.contains(record_tweet_rt.get("id_tweet_rt").asString())){
				tweets_rt=tweets_rt + 1;
				id_de_tweet_rt.add(record_tweet_rt.get("id_tweet_rt").asString());
			}
        }
		System.out.println("\nTiene "+tweets_rt+" tweets rt");
		
		
		//nùmero de tweet replicados
		StatementResult result_tweets_re = session.run(String.format("MATCH (a:User) where a.id='%s' match (b:User)-[r:reply]->(a) return r.id as id_tweet_re", id_autor));
		int tweets_re = 0;
		List <String> id_de_tweet_re = new ArrayList<String>();
		while (result_tweets_re.hasNext() )
        {
			Record record_tweet_re = result_tweets_re.next();
			if(!id_de_tweet_re.contains(record_tweet_re.get("id_tweet_re").asString())){
				tweets_re=tweets_re + 1;
				id_de_tweet_re.add(record_tweet_re.get("id_tweet_re").asString());
			}
        }
		System.out.println("Tiene "+tweets_re+" tweets re");
		
		
		
		//nùmero de tweet
		StatementResult result_tweets = session.run(String.format("MATCH (a:User) where a.id='%s' match (a)-[r]->(b) return r.id as id_tweet", id_autor));
		int tweets = tweets_rt;
		//List <String> id_de_tweet_rt = new ArrayList<String>();
		while (result_tweets.hasNext() )
        {
			Record record_tweet = result_tweets.next();
			if(!id_de_tweet_rt.contains(record_tweet.get("id_tweet").asString())){
				tweets=tweets + 1;
				id_de_tweet_rt.add(record_tweet.get("id_tweet").asString());
			}
        }
		System.out.println("Tiene "+tweets+" tweets");
		
		float RMr = 0;
		if(tweets != 0){
			RMr = (tweets_rt + tweets_re)/tweets;
		}
		System.out.println("\nRMr "+RMr);
		
		float SNP = (Ir + RMr)/2;
		System.out.println("\nSNP "+SNP);
	}

	
	
	

}
