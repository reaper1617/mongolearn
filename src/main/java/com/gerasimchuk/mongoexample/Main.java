package com.gerasimchuk.mongoexample;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

    private static final String mongoUrl = "mongodb+srv://admin:admin7350@cluster0.hbz8l.mongodb.net/mflix?retryWrites=true&w=majority";

    private static final String databaseName = "mflix";
    private static final String collectionName = "movies_initial";

    private static final MongoClient MONGO_CLIENT;

    static {
        MONGO_CLIENT = MongoClients.create(mongoUrl);
    }


    public static void main(String[] args) {
        listDatabases();
        writeSingleDocument();
        writeBulkDocuments();
        getOneWithFilter();
        getListWithFilter();
        getListWithFilterHelperMethods();
        getListWithFilterGreaterThanOrEqual();
        getListWithFilterGreaterThan();
        iterateOverMongoCollection();
    }

    private static Document createRandomMovie() {
        Document movie = new Document("_id", new ObjectId());
        movie.append("imdbId", new Random().nextInt(5))
                .append("title", "Movie_movie")
                .append("year", "2021");
        return movie;
    }

    private static void listDatabases() {
        System.out.println("Databases from mongo:");
        MONGO_CLIENT
                .listDatabases()
                .into(new ArrayList<>())
                .forEach(db -> System.out.println("Database" + db));
    }

    private static void writeSingleDocument() {
        System.out.println("Creating single test movie document:");
        MongoCollection<Document> moviesCollection = getCollection();
        Document movie = createRandomMovie();
        moviesCollection.insertOne(movie);
        System.out.println("Single document created successfully");
    }

    private static void writeBulkDocuments() {
        int num = new Random().nextInt(10) + 1;
        System.out.println("Creating bulk movies of num: " + num);

        List<Document> movies = Stream.generate(() -> new Document("_id", new ObjectId())
                .append("imdbId", new Random().nextInt(5))
                .append("title", "Movie_movie")
                .append("year", "2021"))
                .limit(num)
                .collect(Collectors.toList());
        System.out.println("Movies generated: " + movies);
        System.out.println("Inserting into mongo ...");
        MongoCollection<Document> moviesCollection = getCollection();
        moviesCollection.insertMany(movies);
        System.out.println("Inserted successfully");
    }

    private static void getOneWithFilter() {
        System.out.println("Trying to get single doc with filter");
        Document doc = getCollection().find(new Document("title", "Movie_movie")).first();
        System.out.println("Found: " + doc);
    }

    private static void getListWithFilter() {
        System.out.println("Trying to get docs list with filter");
        List<Document> docs = getCollection().find(new Document("title", "Movie_movie")).limit(10).into(new ArrayList<>());
        System.out.println("Found: " + docs);
    }

    private static void getListWithFilterHelperMethods() {
        System.out.println("Trying to get docs list with filters.eq predicate");
        List<Document> docs = getCollection()
                .find(Filters.eq("title", "Movie_movie"))
                .limit(10)
                .into(new ArrayList<>());
        System.out.println("Found: " + docs);
    }

    private static void getListWithFilterGreaterThanOrEqual() {
        System.out.println("Trying to get docs list with filters.gte predicate");
        List<Document> docs = getCollection()
                .find(Filters.gte("year", "2021"))
                .limit(10)
                .into(new ArrayList<>());
        System.out.println("Found: " + docs);
    }

    private static void getListWithFilterGreaterThan() {
        System.out.println("Trying to get docs list with filters.gte predicate");
        List<Document> docs = getCollection()
                .find(Filters.gt("year", "2021"))
                .limit(10)
                .into(new ArrayList<>());
        System.out.println("Found: " + docs);
    }

    private static void iterateOverMongoCollection() {
        System.out.println("Trying to iterate over mongo collection");
        FindIterable<Document> iterable = getCollection().find(Filters.eq("year", "2021"));
        MongoCursor<Document> cursor = iterable.iterator();
        System.out.println("Iterating with mongo cursor");
        while (cursor.hasNext()){
            System.out.println(cursor.next().toJson());
        }
        System.out.println("End iterating");
    }

    private static MongoCollection<Document> getCollection() {
        MongoDatabase mflixDatabase = MONGO_CLIENT.getDatabase(databaseName);
        return mflixDatabase.getCollection(collectionName);
    }


}
