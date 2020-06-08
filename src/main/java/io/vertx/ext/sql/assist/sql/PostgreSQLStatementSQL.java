package io.vertx.ext.sql.assist.sql;

import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.assist.SqlAndParams;
import io.vertx.ext.sql.assist.SqlPropertyValue;

import java.util.List;

/**
 * SQLite通用SQL操作
 * 
 * @author <a href="http://szmirren.com">Mirren</a>
 *
 */
public class PostgreSQLStatementSQL extends AbstractStatementSQL {
	private final Logger LOG = LoggerFactory.getLogger(PostgreSQLStatementSQL.class);

	public PostgreSQLStatementSQL(Class<?> entity) {
		super(entity);
	}

	@Override
	public AbstractStatementSQL setSqlTableName(String sqlTableName) {
		return super.setSqlTableName(sqlTableName);
	}

	@Override
	protected String getSqlTableColumns(String value) {
		return "\""+value+"\"";
	}

	@Override
	protected String getSqlPrimaryIdAlias(String value) {
		return "\""+value+"\"";
	}

	@Override
	protected String getSqlPrimaryId(String value) {
		return "\""+value+"\"";
	}

	@Override
	protected String getSqlTableName(String value) {
		return "\""+value+"\"";
	}

	@Override
	protected String getSqlTableColumnsAlias(String alias) {
		return "\""+alias+"\"";
	}

	@Override
	public <T> SqlAndParams upsertAllSQL(T obj) {
		JsonArray params = null;
		StringBuilder tempColumn = null;
		StringBuilder tempValues = null;
		StringBuilder updateItems = null;
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (tempColumn == null) {
				updateItems = new StringBuilder(pv.getName()).append("=").append("excluded.").append(pv.getName());
				tempColumn = new StringBuilder(pv.getName());
				tempValues = new StringBuilder("?");
				params = new JsonArray();
			} else {
				updateItems.append(",").append(pv.getName()).append("=").append("excluded.").append(pv.getName());
				tempColumn.append(",").append(pv.getName());
				tempValues.append(",?");
			}
			if (pv.getValue() != null) {
				params.add(pv.getValue());
			} else {
				params.addNull();
			}
		}
		String sql = String.format("insert into %s (%s) values (%s) ON CONFLICT(%s) do update set %s",
				getSqlTableName(), tempColumn, tempValues,getSqlPrimaryId(),updateItems);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (LOG.isDebugEnabled()) {
			LOG.debug("insertAllSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams insertAllSQLReturnId(T obj) {
		JsonArray params = null;
		StringBuilder tempColumn = null;
		StringBuilder tempValues = null;
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (tempColumn == null) {
				tempColumn = new StringBuilder(pv.getName());
				tempValues = new StringBuilder("?");
				params = new JsonArray();
			} else {
				tempColumn.append(",").append(pv.getName());
				tempValues.append(",?");
			}
			if (pv.getValue() != null) {
				params.add(pv.getValue());
			} else {
				params.addNull();
			}
		}
		String sql = String.format("insert into %s (%s) values (%s) returning %s", getSqlTableName(), tempColumn, tempValues,getSqlPrimaryId());
		SqlAndParams result = new SqlAndParams(sql, params);
		if (LOG.isDebugEnabled()) {
			LOG.debug("insertAllSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams upsertAllSQLReturnId(T obj) {
		JsonArray params = null;
		StringBuilder tempColumn = null;
		StringBuilder tempValues = null;
		StringBuilder updateItems = null;
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (tempColumn == null) {
				updateItems = new StringBuilder(pv.getName()).append("=").append("excluded.").append(pv.getName());
				tempColumn = new StringBuilder(pv.getName());
				tempValues = new StringBuilder("?");
				params = new JsonArray();
			} else {
				updateItems.append(",").append(pv.getName()).append("=").append("excluded.").append(pv.getName());
				tempColumn.append(",").append(pv.getName());
				tempValues.append(",?");
			}
			if (pv.getValue() != null) {
				params.add(pv.getValue());
			} else {
				params.addNull();
			}
		}
		String sql = String.format("insert into %s (%s) values (%s) ON CONFLICT(%s) do update set %s returning %s",
				getSqlTableName(), tempColumn, tempValues,getSqlPrimaryId(),updateItems,getSqlPrimaryId());
		SqlAndParams result = new SqlAndParams(sql, params);
		if (LOG.isDebugEnabled()) {
			LOG.debug("insertAllSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams upsertNonEmptySQL(T obj) {
		JsonArray params = null;
		StringBuilder tempColumn = null;
		StringBuilder tempValues = null;
		StringBuilder updateItems = null;
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (pv.getValue() != null) {
				if (tempColumn == null) {
					updateItems = new StringBuilder(pv.getName()).append("=").append("excluded.").append(pv.getName());
					tempColumn = new StringBuilder(pv.getName());
					tempValues = new StringBuilder("?");
					params = new JsonArray();
				} else {
					updateItems.append(",").append(pv.getName()).append("=").append("excluded.").append(pv.getName());
					tempColumn.append(",").append(pv.getName());
					tempValues.append(",?");
				}
				params.add(pv.getValue());
			}
		}
		String sql = String.format("insert into %s (%s) values (%s) ON CONFLICT(%s) do update set %s", getSqlTableName(), tempColumn, tempValues,getSqlPrimaryId(),updateItems);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("insertNonEmptySQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams insertNonEmptySQLReturnId(T obj) {
		JsonArray params = null;
		StringBuilder tempColumn = null;
		StringBuilder tempValues = null;
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (pv.getValue() != null) {
				if (tempColumn == null) {
					tempColumn = new StringBuilder(pv.getName());
					tempValues = new StringBuilder("?");
					params = new JsonArray();
				} else {
					tempColumn.append(",").append(pv.getName());
					tempValues.append(",?");
				}
				params.add(pv.getValue());
			}
		}
		String sql = String.format("insert into %s (%s) values (%s) returning %s", getSqlTableName(), tempColumn, tempValues,getSqlPrimaryId());
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("insertNonEmptySQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams upsertNonEmptySQLReturnId(T obj) {
		JsonArray params = null;
		StringBuilder tempColumn = null;
		StringBuilder tempValues = null;
		StringBuilder updateItems = null;
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (pv.getValue() != null) {
				if (tempColumn == null) {
					updateItems = new StringBuilder(pv.getName()).append("=").append("excluded.").append(pv.getName());
					tempColumn = new StringBuilder(pv.getName());
					tempValues = new StringBuilder("?");
					params = new JsonArray();
				} else {
					updateItems.append(",").append(pv.getName()).append("=").append("excluded.").append(pv.getName());
					tempColumn.append(",").append(pv.getName());
					tempValues.append(",?");
				}
				params.add(pv.getValue());
			}
		}
		String sql = String.format("insert into %s (%s) values (%s) ON CONFLICT(%s) do update set %s returning %s",
				getSqlTableName(), tempColumn, tempValues,getSqlPrimaryId(),updateItems,getSqlPrimaryId());
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("insertNonEmptySQL : " + result.toString());
		}
		return result;
	}

	@Override
	protected Logger getLOG() {
		return LOG;
	}
}
