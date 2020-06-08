package io.vertx.ext.sql.assist;

import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.sql.assist.sql.PostgreSQLStatementSQL;

/**
 * JDBCClient版的SQL实现
 *
 * @author <a href="http://szmirren.com">Mirren</a>
 */
public class SQLExecuteImpl implements SQLExecute<SQLClient> {
    private Logger logger;
    /**
     * SQL客户端
     */
    private final SQLClient client;

    public SQLExecuteImpl(SQLClient client) {
        super();
        this.client = client;
    }

    @Override
    public SQLClient getClient() {
        return client;
    }

    @Override
    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    @Override
    public Future<JsonObject> queryAsObj(SqlAndParams qp) {
        return this.queryExecute(qp)
                .map(ResultSet::getRows)
                .map(rows -> {
                    if (rows != null && !rows.isEmpty())
                        return rows.get(0);
                    else
                        return null;
                });
    }

    @Override
    public Future<List<JsonObject>> queryAsListObj(SqlAndParams qp) {
        return this.queryExecute(qp)
                .map(ResultSet::getRows);
    }

    @Override
    public Future<List<JsonArray>> queryAsListArray(SqlAndParams qp) {
        return this.queryExecute(qp)
                .map(ResultSet::getResults);
    }

    @Override
    public Future<JsonArray> insert(SqlAndParams qp) {
        if (PostgreSQLStatementSQL.class.getName().equals(SQLStatement.getStatementClassName()))
            return this.queryExecute(qp)
                    .map(resultSet -> resultSet.getResults().get(0));
        else
        	return this.updateExecute(qp)
					.map(UpdateResult::getKeys);
    }

    @Override
    public Future<Integer> update(SqlAndParams qp) {
        return this.updateExecute(qp)
                .map(UpdateResult::getUpdated);
    }

    @Override
    public Future<List<Integer>> batch(SqlAndParams qp) {
        Future<List<Integer>> result = Future.future();
        client.getConnection(conn -> {
            if (conn.succeeded()) {
                SQLConnection connection = conn.result();
                connection.batchWithParams(qp.getSql(), qp.getBatchParams(), res -> {
                    if (res.succeeded()) {
                        connection.close(close -> {
                            if (close.succeeded()) {
                                result.handle(Future.succeededFuture(res.result()));
                            } else {
                                result.handle(Future.failedFuture(close.cause()));
                            }
                        });
                    } else {
                        result.handle(Future.failedFuture(res.cause()));
                        connection.close();
                    }
                });
            } else {
                result.handle(Future.failedFuture(conn.cause()));
            }
        });
        return result;
    }

    /**
     * 执行查询
     *
     * @param qp
     */
    public Future<ResultSet> queryExecute(SqlAndParams qp) {
        Future<ResultSet> result = Future.future();
        this.logSqlAndParam(qp.getSql(), qp.getParams());
        if (qp.getParams() == null) {
            client.query(qp.getSql(), result);
        } else {
            client.queryWithParams(qp.getSql(), qp.getParams(), result);
        }
        return result;
    }

    /**
     * 执行更新
     *
     * @param qp
     */
    public Future<UpdateResult> updateExecute(SqlAndParams qp) {
        Future<UpdateResult> result = Future.future();
        this.logSqlAndParam(qp.getSql(), qp.getParams());
        if (qp.getParams() == null) {
            client.update(qp.getSql(), result);
        } else {
            client.updateWithParams(qp.getSql(), qp.getParams(), result);
        }
        return result;
    }

    private void logSqlAndParam(String sql, JsonArray params) {
        if (logger == null) return;
        logger.info(sql);
        logger.info(params.toString());
    }
}
