package io.vertx.ext.sql.assist;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.*;

/**
 * JDBCClient版的SQL实现
 *
 * @author <a href="http://szmirren.com">Mirren</a>
 */
public class SQLExecuteMySQLImpl implements SQLExecute<MySQLPool> {
    /**
     * MySQL客户端
     */
    private MySQLPool client;

    public SQLExecuteMySQLImpl(MySQLPool client) {
        super();
        this.client = client;
    }

    @Override
    public MySQLPool getClient() {
        return client;
    }

    @Override
    public void setLogger(Logger logger) {

    }

    @Override
    public Future<JsonObject> queryAsObj(SqlAndParams qp) {
        return this.queryExecute(qp)
                .map(result -> {
                    List<String> names = result.columnsNames();
                    List<JsonObject> rows = new ArrayList<>();
                    for (Row row : result) {
                        JsonObject data = new JsonObject();
                        for (int i = 0; i < names.size(); i++) {
                            data.put(names.get(i), row.getValue(i));
                        }
                        rows.add(data);
                    }
                    if (rows.size() > 0) {
                        return rows.get(0);
                    } else {
                        return null;
                    }
                });
    }

    @Override
    public Future<List<JsonObject>> queryAsListObj(SqlAndParams qp) {
        return this.queryExecute(qp).map(result -> {
            List<String> names = result.columnsNames();
            List<JsonObject> rows = new ArrayList<>();
            for (Row row : result) {
                JsonObject data = new JsonObject();
                for (int i = 0; i < names.size(); i++) {
                    data.put(names.get(i), row.getValue(i));
                }
                rows.add(data);
            }
            return rows;
        });
    }

    @Override
    public Future<List<JsonArray>> queryAsListArray(SqlAndParams qp) {
        return this.queryExecute(qp).map(result->{
			List<JsonArray> rows = new ArrayList<>();
			for (Row row : result) {
				JsonArray data = new JsonArray();
				for (int i = 0; i < row.size(); i++) {
					data.add(row.getValue(i));
				}
				rows.add(data);
			}
			return rows;
		});
    }

	@Override
	public Future<JsonArray> insert(SqlAndParams qp) {
		return this.updateExecute(qp)
				.map(res-> new JsonArray().add(res.property(MySQLClient.LAST_INSERTED_ID)));
	}

	@Override
    public Future<Integer> update(SqlAndParams qp) {
        return updateExecute(qp).map(SqlResult::rowCount);
    }

    @Override
    public Future<List<Integer>> batch(SqlAndParams qp) {
    	Promise<List<Integer>> result = Promise.promise();
        if (qp.succeeded()) {
            client.getConnection(conn -> {
                if (conn.succeeded()) {
                    SqlConnection connection = conn.result();
                    List<Tuple> batch = new ArrayList<>();
                    List<JsonArray> params = qp.getBatchParams();
                    for (JsonArray param : params) {
                        @SuppressWarnings("unchecked")
                        List<Object> list = param.getList();
                        batch.add(Tuple.tuple(list));
                    }
                    connection.preparedQuery(qp.getSql()).executeBatch(batch, res -> {
						if (res.succeeded()) {
							result.complete(new ArrayList<>());
							connection.close();
						} else {
							result.fail(res.cause());
							connection.close();
						}
					});
                } else {
                    result.fail(conn.cause());
                }
            });
        } else {
            return Future.failedFuture(qp.getSql());
        }
        return result.future();
    }

    /**
     * 执行查询
     *
     * @param qp
     * @param handler
     */
    public Future<RowSet<Row>> queryExecute(SqlAndParams qp) {
        Promise<RowSet<Row>> promise = Promise.promise();
        if (qp.succeeded()) {
            if (qp.getParams() == null) {
                client.query(qp.getSql()).execute(promise);
            } else {
                @SuppressWarnings("unchecked")
                List<Object> list = qp.getParams().getList();
                client.preparedQuery(qp.getSql()).execute(Tuple.tuple(list), promise);
            }
        } else {
            promise.fail(qp.getSql());
        }
        return promise.future();
    }

    /**
     * 执行更新
     *
     * @param qp
     * @param handler
     */
    public Future<RowSet<Row>> updateExecute(SqlAndParams qp) {
        Promise<RowSet<Row>> result = Promise.promise();
        if (qp.succeeded()) {
            if (qp.getParams() == null) {
                client.query(qp.getSql()).execute(result);
            } else {
                @SuppressWarnings("unchecked")
                List<Object> list = qp.getParams().getList();
                client.preparedQuery(qp.getSql()).execute(Tuple.tuple(list), result);
            }
        } else {
            result.fail(qp.getSql());
        }
        return result.future();
    }

}
