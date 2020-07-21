package io.vertx.ext.sql.assist;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.asyncsql.PostgreSQLClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.assist.core.SQLExecute;
import io.vertx.ext.sql.assist.core.SQLStatement;
import io.vertx.ext.sql.assist.core.SqlAssist;
import io.vertx.ext.sql.assist.sql.MySQLStatementSQL;
import io.vertx.ext.sql.assist.sql.PostgreSQLStatementSQL;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class SQLClientTest {

    public static SQLClient getPostgresClient() {
        Vertx vertx = Vertx.vertx();
        JsonObject config = new JsonObject()
                .put("host", "localhost")
                .put("port", 5432)
                .put("database", "postgres")
                .put("username", "postgres")
                .put("password", "123456789");
        SQLClient client = PostgreSQLClient.createShared(vertx, config);
        SQLStatement.register(PostgreSQLStatementSQL.class);
        return client;
    }

    public static SQLClient getMysqlClient() {
        Vertx vertx = Vertx.vertx();
        JsonObject config = new JsonObject()
                .put("host", "localhost")
                .put("port", 3306)
                .put("database", "test")
                .put("username", "root")
                .put("password", "123456789");
        SQLClient client = MySQLClient.createShared(vertx, config);
        SQLStatement.register(MySQLStatementSQL.class);
        return client;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        SQLClient client = getPostgresClient();
        UserSQL userSql = new UserSQL(SQLExecute.createPostgres(client));
        User user = new User();
        user.setName("test");
        user.setPwd("123456");

        List<JsonObject> res = userSql.selectAll(new SqlAssist().andIn("id",1,2,3)).toCompletionStage().toCompletableFuture().get();

        //
        JsonArray returnId = userSql.insertNonEmptyReturnId(user).toCompletionStage().toCompletableFuture().get();

        //
        userSql.getCount().toCompletionStage().toCompletableFuture().get();

        //
        userSql.getCount(new SqlAssist().andLte("id", 10)).toCompletionStage().toCompletableFuture().get();

        //
        userSql.selectAll().toCompletionStage().toCompletableFuture().get();

        //
        userSql.selectAll(new SqlAssist().setStartRow(0).setRowSize(5)).toCompletionStage().toCompletableFuture().get();

        //
        userSql.deleteById(1).toCompletionStage().toCompletableFuture().get();

        //
        userSql.deleteByAssist(new SqlAssist().andEq("id", 2)).toCompletionStage().toCompletableFuture().get();

        //
        User user2 = new User();
        user2.setId(3L);
        user2.setName("update name3");
        userSql.updateNonEmptyById(user2).toCompletionStage().toCompletableFuture().get();

        User user3 = new User();
        user3.setName("update name4");
        userSql.updateNonEmptyByAssist(user3, new SqlAssist().andEq("id", 4L)).toCompletionStage().toCompletableFuture().get();

        synchronized (SQLClientTest.class) {
            SQLClientTest.class.wait();
        }
    }
}
