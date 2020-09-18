package io.vertx.ext.sql.assist.core;

import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLOperations;
import io.vertx.ext.sql.assist.sql.MySQLStatementSQL;
import io.vertx.ext.sql.assist.sql.PostgreSQLStatementSQL;

/**
 * SQL执行器
 *
 * @author <a href="http://szmirren.com">Mirren</a>
 *
 */
public interface SQLExecute<T> {
	/**
	 * 通过SQL客户端创建一个实例
	 *
	 * @param client
	 * @return
	 */
	static SQLExecute<SQLOperations> create(SQLOperations client) {
		return new SQLExecuteImpl(client);
	}

	static SQLExecute<SQLOperations> createMySql(SQLOperations client) {
		SQLStatement.register(MySQLStatementSQL.class);
		return new SQLExecuteMysqlImpl(client);
	}

	static SQLExecute<SQLOperations> createPostgres(SQLOperations client) {
		SQLStatement.register(PostgreSQLStatementSQL.class);
		return new SQLExecutePostgresqlImpl(client);
	}

	/**
	 * 获取客户端
	 *
	 * @return
	 */
	T getClient();
	/**
	 * 执行查询
	 *
	 * @param qp
	 *          SQL语句与参数
	 * @return future
	 *          返回结果
	 */
	Future<JsonObject> queryAsObj(SqlAndParams qp);

	/**
	 * 执行查询
	 *
	 * @param qp
	 *          SQL语句与参数
	 * @return future
	 *          返回结果
	 */
	Future<List<JsonObject>> queryAsListObj(SqlAndParams qp);

	/**
	 * 执行查询
	 *
	 * @param qp
	 *          SQL语句与参数
	 * @return future
	 *          返回结果
	 */
	Future<List<JsonArray>> queryAsListArray(SqlAndParams qp);

	/**
	 * 执行更新等操作得到受影响的行数
	 *
	 * @param qp
	 *          SQL语句与参数
	 *
	 * @return future
	 */
	Future<JsonArray> insert(SqlAndParams qp);

	/**
	 * 执行更新等操作得到受影响的行数
	 *
	 * @param qp
	 *          SQL语句与参数
	 *
	 * @return future
	 */
	Future<Integer> update(SqlAndParams qp);

}
