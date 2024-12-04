package api;

import data.Message;
import data.MessageService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.jboss.logging.Logger;

import java.util.List;

@Path("/api")
public class ApiEndpoint {

    @Inject
    Logger logger;

    @Inject
    MessageService messageService;

    @GET
    @Path("hello")
    public String hello() {
        logger.info("hello()");
        return "Ok";
    }

    @GET
    @Path("async/hello")
    public Uni<String> asyncHello() {
        logger.info("async-hello()");
        return Uni.createFrom().item("Ok")
                .onItem().invoke(() -> logger.info("async-hello"));
    }

    @GET
    @Path("all")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Message> syncFetch() {
        return messageService.fetchAllSync();
    }

    @GET
    @Path("async/all")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<List<Message>> asyncFetch() {
        return messageService.fetchAllAsync();
    }

    @GET
    @Path("async/all-bad")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<List<Message>> asyncFetchButBadly() {
        return messageService.fetchAllAsyncButBadly();
    }
}
