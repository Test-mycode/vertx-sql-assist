package io.vertx.ext.sql.assist.sql;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.assist.SqlAndParams;

/**
 * SQLite通用SQL操作
 * 
 * @author <a href="http://szmirren.com">Mirren</a>
 *
 */
public class SQLiteStatementSQL extends AbstractStatementSQL {

	private static final Logger LOG = LoggerFactory.getLogger(SqlServerStatementSQL.class);

	public SQLiteStatementSQL(Class<?> entity) {
		super(entity);
	}

	@Override
	protected Logger getLOG() {
		return LOG;
	}

	@Override
	public <T> SqlAndParams upsertAllSQL(T obj) {
		throw new UnsupportedOperationException("未实现");
	}

	@Override
	public <T> SqlAndParams insertAllSQLReturnId(T obj) {
		throw new UnsupportedOperationException("未实现");
	}

	@Override
	public <T> SqlAndParams upsertAllSQLReturnId(T obj) {
		throw new UnsupportedOperationException("未实现");
	}

	@Override
	public <T> SqlAndParams upsertNonEmptySQL(T obj) {
		throw new UnsupportedOperationException("未实现");
	}

	@Override
	public <T> SqlAndParams insertNonEmptySQLReturnId(T obj) {
		throw new UnsupportedOperationException("未实现");
	}

	@Override
	public <T> SqlAndParams upsertNonEmptySQLReturnId(T obj) {
		throw new UnsupportedOperationException("未实现");
	}
}
