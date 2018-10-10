package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.PostgreSQLClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainVerticle extends AbstractVerticle {

    private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists pages "
            +" (id SERIAL PRIMARY KEY, name varchar(255) unique, content text)";
    private static final String SQL_GET_PAGE = "select id, content from pages where name = ?";
    private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
    private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
    private static final String SQL_ALL_PAGES = "select name from pages";
    private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";
    private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

    private SQLClient sqlClient;
    private final FreeMarkerTemplateEngine templateEngine = FreeMarkerTemplateEngine.create();

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Future<Void> steps = prepareDatabase().compose(v -> startHttpServer());
        steps.setHandler(ar -> {
            if (ar.succeeded()) {
                startFuture.complete();
                LOGGER.info("MainVerticle started...");
            } else {
                startFuture.fail(ar.cause());
            }
        });

        /*
         * vertx.createHttpServer().requestHandler(req -> { req.response()
         * .putHeader("content-type", "text/plain") .end("Hello from Vert.x!");
         * }).listen(8080, http -> { if (http.succeeded()) { startFuture.complete();
         * System.out.println("HTTP server started on http://localhost:8080"); } else {
         * startFuture.fail(http.cause()); } });
         */

    }

    private Future<Void> startHttpServer() {
        Future<Void> future = Future.future();
        HttpServer server = vertx.createHttpServer();

        Router router = Router.router(vertx);
        router.get("/").handler(this::indexHandler);

        server
            .requestHandler(router::accept)
            .listen(8080, ar ->{
                if (ar.succeeded()) {
                    LOGGER.info("Started HTTP server on port 8080");
                    future.complete();
                } else {
                    LOGGER.error("Failed to start a HTTP server", ar.cause());
                    future.failed();
                }
            });

        return future;
    }

    /*  DB Config options
     *  {
          "host" : <your-host>,
          "port" : <your-port>,
          "maxPoolSize" : <maximum-number-of-open-connections>,
          "username" : <your-username>,
          "password" : <your-password>,
          "database" : <name-of-your-database>,
          "charset" : <name-of-the-character-set>,
          "queryTimeout" : <timeout-in-milliseconds>,
          "sslMode" : <"disable"|"prefer"|"require"|"verify-ca"|"verify-full">,
          "sslRootCert" : <path to file with certificate>
        }
     */
    private Future<Void> prepareDatabase() {
        Future<Void> future = Future.future();
        JsonObject config = new JsonObject()
                .put("host", "localhost")
                .put("username", "postgres")
                .put("password", "password")
                .put("database", "postgres")
                .put("max_pool_size", 10);
        final String poolName = "PostgresSharedPool1";

        sqlClient = PostgreSQLClient.createShared(vertx, config, poolName);
        sqlClient.getConnection(ar -> {
            if (ar.succeeded()) {
                SQLConnection connection = ar.result();
                connection.execute(SQL_CREATE_PAGES_TABLE, sqlAr -> {
                    connection.close();
                    if (sqlAr.succeeded()) {
                        future.complete();
                    } else {
                        LOGGER.error("Failed to create table in database", sqlAr.cause());
                        future.fail(sqlAr.cause());
                    }
                });
            } else {
                LOGGER.error("Failed to create database connection", ar.cause());
                future.fail(ar.cause());
            }
        });

        return future;
    }

    private void indexHandler(RoutingContext context) {
        sqlClient.getConnection(conAr -> {
            if (conAr.succeeded()) {
              SQLConnection connection = conAr.result();
              connection.query(SQL_ALL_PAGES, res -> {
                connection.close();

                if (res.succeeded()) {
                  List<String> pages = res.result()
                    .getResults()
                    .stream()
                    .map(json -> json.getString(0))
                    .sorted()
                    .collect(Collectors.toList());

                  context.put("title", "Wiki home");
                  context.put("pages", pages);
                  templateEngine.render(context, "templates", "/index.ftl", ar -> {
                    if (ar.succeeded()) {
                      context.response().putHeader("Content-Type", "text/html");
                      context.response().end(ar.result());
                    } else {
                      context.fail(ar.cause());
                    }
                  });

                } else {
                  context.fail(res.cause());
                }
              });
            } else {
              context.fail(conAr.cause());
            }
          });
    }

}
