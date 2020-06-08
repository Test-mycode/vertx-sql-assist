package io.vertx.ext.sql.assist;

import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.ext.sql.SQLClient;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.pgclient.PgPool;

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
	static SQLExecute<SQLClient> create(SQLClient client) {
		return new SQLExecuteImpl(client);
	}

	static SQLExecute<MySQLPool> createMySql(MySQLPool client) {
		return new SQLExecuteMySQLImpl(client);
	}

	static SQLExecute<PgPool> createPg(PgPool client) {
		return new SQLExecutePgImpl(client);
	}

	/**
	 * 获取客户端
	 * 
	 * @return
	 */
	T getClient();

	/*
	*  添加日志
	*
	* */
	void setLogger(Logger logger);

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

	/**
	 * 批量操作
	 * 
	 * @param qp
	 *          SQL语句与批量参数
	 * 
	 * @return future
	 *          返回结果
	 */
	Future<List<Integer>> batch(SqlAndParams qp);

}
