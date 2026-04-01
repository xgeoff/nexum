package biz.digitalindustry.storage.server.controller;

import biz.digitalindustry.storage.server.model.MaintenanceStatusResponse;
import biz.digitalindustry.storage.server.service.GraphStore;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;

@Controller("/admin/maintenance")
public class MaintenanceController {
    private final GraphStore graphStore;

    public MaintenanceController(GraphStore graphStore) {
        this.graphStore = graphStore;
    }

    @Get
    @Produces(MediaType.APPLICATION_JSON)
    public MaintenanceStatusResponse status() {
        return new MaintenanceStatusResponse(
                graphStore.walSizeBytes(),
                graphStore.needsCheckpoint()
        );
    }

    @Post("/checkpoint")
    @Produces(MediaType.APPLICATION_JSON)
    public MaintenanceStatusResponse checkpoint() {
        graphStore.checkpoint();
        return status();
    }
}
