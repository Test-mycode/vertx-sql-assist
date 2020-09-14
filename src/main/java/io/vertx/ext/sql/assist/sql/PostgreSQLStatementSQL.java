package io.vertx.ext.sql.assist.sql;

import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.assist.core.SqlAndParams;
import io.vertx.ext.sql.assist.core.SqlPropertyValue;

import java.util.LinkedList;
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
	protected String getAliasNameValue(String value) {
		return "\""+value+"\"";
	}

	@Override
	protected String getNameValue(String value) {
		return "\""+value+"\"";
	}

	@Override
	public <T> SqlAndParams upsertAllSQL(T obj, String dupCol) {
		if(dupCol==null||dupCol.trim().equals(""))
			dupCol = this.sqlPrimaryId;
		JsonArray params = new JsonArray();
		LinkedList<String> tempColumn = new LinkedList<>();
		LinkedList<String> tempValues = new LinkedList<>();
		LinkedList<String> updateItems = new LinkedList<>();
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			tempColumn.add(pv.getName());
			tempValues.add("?");
			updateItems.add(pv.getName()+" = excluded."+pv.getName()+" ");
			if (pv.getValue() != null) {
				params.add(pv.getValue());
			} else {
				params.addNull();
			}
		}
		String sql = String.format("insert into %s (%s) values (%s) ON CONFLICT(%s) do update set %s",
				this.sqlTableName, String.join(",",tempColumn), String.join(",",tempValues), dupCol,String.join(",",updateItems));
		SqlAndParams result = new SqlAndParams(sql, params);
		if (LOG.isDebugEnabled()) {
			LOG.debug("upsertAllSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams upsertNonEmptySQL(T obj, String dupCol) {
		if(dupCol==null||dupCol.trim().equals(""))
			dupCol = this.sqlPrimaryId;

		JsonArray params = new JsonArray();
		LinkedList<String> tempColumn = new LinkedList<>();
		LinkedList<String> tempValues = new LinkedList<>();
		LinkedList<String> updateItems = new LinkedList<>();
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (pv.getValue() != null) {
				tempColumn.add(pv.getName());
				tempValues.add("?");
				updateItems.add(pv.getName()+" = excluded."+pv.getName()+" ");
				params.add(pv.getValue());
			}
		}
		String sql = String.format("insert into %s (%s) values (%s) ON CONFLICT(%s) do update set %s",
				this.sqlTableName,String.join(",",tempColumn), String.join(",",tempValues), dupCol,String.join(",",updateItems));
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("upsertNonEmptySQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams insertNonEmptySQLReturnId(T obj) {
		JsonArray params = new JsonArray();
		LinkedList<String> tempColumn = new LinkedList<>();
		LinkedList<String> tempValues = new LinkedList<>();
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (pv.getValue() != null) {
				tempColumn.add(pv.getName());
				tempValues.add("?");
				params.add(pv.getValue());
			}
		}
		String sql = String.format("insert into %s (%s) values (%s) returning %s", this.sqlTableName, String.join(",",tempColumn), String.join(",",tempValues), this.sqlPrimaryId);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("insertNonEmptySQLReturnId : " + result.toString());
		}
		return result;
	}

	@Override
	protected Logger getLOG() {
		return LOG;
	}
}
