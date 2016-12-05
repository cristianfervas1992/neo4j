package cl.citiaps.neo4j.main;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

public class main2{

    public static void main(String[] args) {
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "0995" ) );
        Session session = driver.session();

        session.run("match (a)-[r]->(b) delete r");
        session.run("match (n) delete n");
        /*
        session.run( "CREATE (a:Person {name:'Arthur', title:'King'})");
        session.run( "CREATE (a:Person {name:'Lancelot', title:'Sir'})");
        session.run( "CREATE (a:Person {name:'Merlin', title:'Wizard'})");
        */
        
        StatementResult result = session.run( "MATCH (a:Person) return a.name as name, a.title as title");
        /*while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println( record.get( "title" ).asString() + " " + record.get("name").asString() );
        }
        
        session.run("match (a:Person) where a.name='Lancelot' "
                + "  match (b:Person) where b.name='Arthur' "
                + "  create (a)-[r:Loyal {reason:'He love Arthur in secret :3'}]->(b)");
        
        session.run("match (a:Person) where a.name='Merlin'"
                + "match (b:Person) where b.name='Arthur'"
                + "create (a)-[r:Advise]->(b)");
        
        session.run("create (a:Person {name:'Guinevere',title:'Lady'})");
        
        session.run("match (a:Person) where a.name='Arthur'"
                + "match (b:Person) where b.name='Guinevere'"
                + "create (a)-[r:Married {since:'13 Century'}]->(b)");
        */
        session.run("create (a:Person {name:'Percival',title:'Sir'})");
        session.run("create (a:Person {name:'Blanchefleur',title:'Lady'})");
        
        session.run("match (a:Person) where a.name='Percival'"
                + "match (b:Person) where b.name='Blanchefleur'"
                + "create (a)-[r:Married]->(b)");
        
        /*session.run("match (a:Person) where a.name='Percival'"
                + "match (b:Person) where b.name='Arthur'"
                + "create (a)-[r:Loyal]->(b)");
        
        session.run("match (a:Person) where a.name='Lancelot' "
                + "  match (b:Person) where b.name='Percival' "
                + "  create (a)-[r:Fellow]->(b)");
        */
        /*result = session.run( "MATCH (a:Person) where a.name='Lancelot' match (a)-[r]->(b:Person) return b.name as name, b.title as title");
        //result = session.run( "MATCH (a:Person) where a.name='Lancelot' match (a)-[r:]->(b:Person) return b.name as name, b.title as title");
        
        while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println( record.get( "title" ).asString() + " " + record.get("name").asString() );
            StatementResult result2 = session.run( "MATCH (a:Person) where a.name='"+record.get("name").asString()+"' match(a)-[r]->(b) return b.name as name, b.title as title");
            while ( result2.hasNext() )
            {
                record = result2.next();
                System.out.println(record.get( "title" ).asString() + " " + record.get("name").asString() );
            }
        }
        
        /*String[] names = new String[2];
        names[0] = "Galahad";
        names[1] = "Bors";
        
        for (String name:names)
        {
            session.run("create (a:Person {name:'"+name+"',title:'Sir'})");
            session.run("match (a:Person) where a.name='Lancelot'"
                    + "match (b:Person) where b.name='"+name+"'"
                    + "create (a)-[r:Fellow]->(b)");
            session.run("match (a:Person) where a.name='Arthur'"
                    + "match (b:Person) where b.name='"+name+"'"
                    + "create (a)-[r:Loyal]->(b)");
        }*/
        
        session.close();
        driver.close();
    }
    
}