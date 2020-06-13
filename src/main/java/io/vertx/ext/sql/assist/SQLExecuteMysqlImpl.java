package io.vertx.ext.sql.assist;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.sql.assist.sql.PostgreSQLStatementSQL;

import java.util.List;

/**
 * JDBCClient版的SQL实现
 *
 * @author <a href="http://szmirren.com">Mirren</a>
 */
public class SQLExecuteMysqlImpl implements SQLExecute<SQLClient> {
    private Logger logger;
    /**
     * SQL客户端
     */
    private final SQLClient client;

    public SQLExecuteMysqlImpl(SQLClient client) {
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
        return this.updateExecute(qp)
                .map(UpdateResult::getKeys);
    }

    @Override
    public Future<Integer> update(SqlAndParams qp) {
        return this.updateExecute(qp)
                .map(UpdateResult::getUpdated);
    }

    /**
     * 执行查询
     *
     * @param qp
     */
    public Future<ResultSet> queryExecute(SqlAndParams qp) {
        Promise<ResultSet> result = Promise.promise();
        this.logSqlAndParam(qp.getSql(), qp.getParams());
        if (qp.getParams() == null) {
            client.query(qp.getSql(), result);
        } else {
            client.queryWithParams(qp.getSql(), qp.getParams(), result);
        }
        return result.future();
    }

    /**
     * 执行更新
     *
     * @param qp
     */
    public Future<UpdateResult> updateExecute(SqlAndParams qp) {
        Promise<UpdateResult> result = Promise.promise();
        this.logSqlAndParam(qp.getSql(), qp.getParams());
        if (qp.getParams() == null) {
            client.update(qp.getSql(), result);
        } else {
            client.updateWithParams(qp.getSql(), qp.getParams(), result);
        }
        return result.future();
    }

    private void logSqlAndParam(String sql, JsonArray params) {
        if (logger == null) return;
        logger.info(sql);
        logger.info(params.toString());
    }
}
