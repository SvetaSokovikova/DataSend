package com.mycompany.datainput;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.BitSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class Main {
    
    public static void main(String[] args){
        
        String database_name = "C:/Films";  //Database name parameter
        
        String bitmapFileName = "C:/Users/User/Desktop/FilmsBitmap.csv"; //Csv-file name parameter
        FileWriter file_writer;
        
        int w = 0; //Only to see the progress in console
        
        try{
            file_writer = new FileWriter(bitmapFileName);
        } 
        catch(Exception e){
            System.out.println("Error in CsvFileCreator!");
            return;
        }
        
        GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
        File location = new File(database_name);
        GraphDatabaseService graphDb = 
                dbFactory.newEmbeddedDatabase(location);
        
        ResourceIterable<String> prop_types;
        ResourceIterable<RelationshipType> rel_types;
        ResourceIterable<Node> all_nodes;
        List prop_attributes;
        List rel_attributes;
        
        try(Transaction tx = graphDb.beginTx()){
            prop_types = graphDb.getAllPropertyKeys();
            rel_types = graphDb.getAllRelationshipTypes();
            all_nodes = graphDb.getAllNodes();
            
            prop_attributes = new ArrayList();
            rel_attributes = new ArrayList();
            
            for (String type: prop_types){
                if (!type.startsWith("type"))
                    prop_attributes.add(type);
            }
        
            for (RelationshipType r_type: rel_types){
                rel_attributes.add(r_type.name());
            }
            
            int n_props = prop_attributes.size();
            
            BitSet bitmap = new BitSet(n_props + rel_attributes.size());
            
            for (Node n: all_nodes){
                bitmap.clear();
                for (String prop: n.getPropertyKeys()){
                    if (prop_attributes.contains(prop))
                        bitmap.set(prop_attributes.indexOf(prop));
                }
                for (Relationship r: n.getRelationships(Direction.OUTGOING)){
                    bitmap.set(rel_attributes.indexOf(r.getType().name()) + n_props);
                }
            
                bitmapToFile(n.getId(),bitmap,n_props + rel_attributes.size(),file_writer);
                
                w++;//Only to show the progress 
                System.out.println(w);//Only to show the progress
               
            }
        }
        catch (Exception e){
            System.out.println("Query to database failed!");
            e.printStackTrace();
        }
        
        try{
            file_writer.flush();
            file_writer.close();   
        }
        catch(Exception e){
            System.out.println("Error in flushing/closing FileWriter!");
        }
        
    }
    
    public static void bitmapToFile(long id, BitSet bitmap, int bitmap_size, FileWriter file_writer){
        
        String to_be_inserted;
        to_be_inserted = String.valueOf(id);
            
        for (int i=0;i<bitmap_size;i++){
            to_be_inserted = to_be_inserted + ",";
            if (bitmap.get(i))
                to_be_inserted = to_be_inserted + "1";
            else to_be_inserted = to_be_inserted + "0";
        }
        
        try{
            file_writer.append(to_be_inserted);
            file_writer.append("\n");
        } catch(Exception e){
            System.out.println("Error in CsvFileWriter!");
        }
    }
    
}
