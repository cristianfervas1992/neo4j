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


@Path("/potential")
@ApplicationPath("/")
@WebService

public class ServicePotential{
	MongoClient mongo = new MongoClient("localhost", 27017);
    DB db = mongo.getDB("twitterdb");
    DBCollection collection = db.getCollection("todo");
    @GET
    @Produces({"application/json"})
    @WebMethod
	//public String obtenerDatos(){ 
		
	//	BasicDBObject fields = new BasicDBObject();
	//	DBCursor cursor = collection.find();
	//	List<DBObject> list = new ArrayList<DBObject>();
	//	while (cursor.hasNext()) {
	//		list.add(cursor.next());
	//	}
      //  return list.toString();
	//}

	@GET
    @Path("compania/{compania}")
    @Produces({"application/json"})
    public String buscarPotencialPorCompania(@PathParam("compania") String compania) {
    	List<DBObject> list = new ArrayList<DBObject>();
        BasicDBObject whereQuery = new BasicDBObject();

      	//jejejej aun no se q poner aquiiii xd  

		DBCursor cursor = collection.find(whereQuery);
		while (cursor.hasNext()) {
			list.add(cursor.next());
		}
        return list.toString();
    }





}
