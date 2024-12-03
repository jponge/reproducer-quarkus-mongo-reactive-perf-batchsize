package data;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.quarkus.mongodb.FindOptions;
import io.quarkus.mongodb.reactive.ReactiveMongoClient;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class MessageService {

    public static final String DB_NAME = "mydb";
    public static final String COLLECTION_NAME = "texts";

    @Inject
    MongoClient mongoClient;

    @Inject
    ReactiveMongoClient reactiveMongoClient;

    @Inject
    Logger logger;

    public List<Message> fetchAllSync() {
        long start = System.currentTimeMillis();
        logger.info("Fetching all messages synchronously");
        ArrayList<Message> messages = mongoClient
                .getDatabase(DB_NAME)
                .getCollection(COLLECTION_NAME, Message.class)
                .find()
                .into(new ArrayList<>());
        logger.info(messages.size() + " messages fetched in " + (System.currentTimeMillis() - start) + "ms");
        return messages;
    }

    public Uni<List<Message>> fetchAllAsync() {
        long start = System.currentTimeMillis();
        logger.info("Fetching all messages asynchronously");
        return reactiveMongoClient
                .getDatabase(DB_NAME)
                .getCollection(COLLECTION_NAME, Message.class)
                .find(new FindOptions().batchSize(4096))
                .collect().asList()
                .onItem().invoke(messages -> logger.info(messages.size() + " messages fetched in " + (System.currentTimeMillis() - start) + "ms"));
    }

    public void startup(@Observes StartupEvent startupEvent) {
        logger.info("Initializing MongoDB dataset");
        MongoDatabase database = mongoClient.getDatabase(DB_NAME);
        if (database.getCollection(COLLECTION_NAME).countDocuments() > 0L) {
            logger.info("MongoDB dataset already initialized");
            return;
        }
        MongoCollection<Message> collection = database.getCollection(COLLECTION_NAME, Message.class);
        final int SIZE = 50_000;
        ArrayList<Message> records = new ArrayList<>(SIZE);
        for (int i = 0; i < SIZE; i++) {
            records.add(new Message(UUID.randomUUID().toString(), "Message #" + i));
        }
        collection.insertMany(records);
        logger.info(records.size() + " messages inserted into MongoDB dataset");
    }
}
