package io.vertx.ext.sql.assist.core;

import io.vertx.core.json.JsonArray;

/**
 * 用于生成数据库语句时返回SQL语句与参数
 * 
 * @author <a href="https://mirrentools.org/">Mirren</a>
 *
 */
public class SqlAndParams {
	/** SQL语句 */
	private String sql;
	/** 参数 */
	private JsonArray params;
	/** 生成语句是否成功 */
	private boolean succeeded = true;
	/**
	 * 创建一个新的SqlAndParams
	 * 
	 * @param sql
	 *          SQL语句
	 */
	public SqlAndParams(String sql) {
		this.sql = sql;
		System.out.println(sql);
	}

	/**
	 * 创建一个新的SqlAndParams
	 * 
	 * @param succeeded
	 *          是否成功,true=成功,false=失败
	 * @param sql
	 *          SQL语句或者失败语句
	 */
	public SqlAndParams(boolean succeeded, String sql) {
		this(sql);
		this.succeeded = succeeded;
	}
	/**
	 * 创建一个新的SqlAndParams
	 * 
	 * @param sql
	 *          SQL语句
	 * @param params
	 *          参数
	 */
	public SqlAndParams(String sql, JsonArray params) {
		this(sql);
		this.params = params;
	}
	/**
	 * 获得SQL语句,如果失败时则为错误语句
	 * 
	 * @return
	 */
	public String getSql() {
		return sql;
	}
	/**
	 * 设置SQL语句,如果失败时则为错误语句
	 * 
	 * @param sql
	 */
	public SqlAndParams setSql(String sql) {
		this.sql = sql;
		return this;
	}
	/**
	 * 获得参数
	 * 
	 * @return
	 */
	public JsonArray getParams() {
		return params;
	}
	/**
	 * 设置参数
	 */
	public SqlAndParams setParams(JsonArray params) {
		this.params = params;
		return this;
	}
	/**
	 * 获取是否成功
	 * 
	 * @return
	 */
	public boolean succeeded() {
		return succeeded;
	}
	/**
	 * 设置是否成功
	 * 
	 * @param succeeded
	 */
	public SqlAndParams setSucceeded(boolean succeeded) {
		this.succeeded = succeeded;
		return this;
	}

	@Override
	public String toString() {
		return "SqlAndParams [sql=" + sql + ", params=" + params + ", succeeded=" + succeeded + "]";
	}

}
