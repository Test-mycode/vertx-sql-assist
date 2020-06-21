package io.vertx.ext.sql.assist.sql;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.ext.sql.assist.core.SQLStatement;
import io.vertx.ext.sql.assist.core.SqlAndParams;
import io.vertx.ext.sql.assist.core.SqlAssist;
import io.vertx.ext.sql.assist.core.SqlPropertyValue;
import io.vertx.ext.sql.assist.core.SqlWhereCondition;
import io.vertx.ext.sql.assist.anno.Table;
import io.vertx.ext.sql.assist.anno.TableColumn;
import io.vertx.ext.sql.assist.anno.TableId;

/**
 * 抽象数据库操作语句,默认以MySQL标准来编写,如果其他数据库可以基础并重写不兼容的方法<br>
 * 通常不支持limit分页的数据库需要重写{@link #selectAllSQL(SqlAssist)}与{@link #selectByObjSQL(Object, String, String, boolean)}这两个方法
 * 
 * @author <a href="https://mirrentools.org/">Mirren</a>
 */
public abstract class AbstractStatementSQL implements SQLStatement {
	/** 表的名称 */
	private String sqlTableName;
	/** 主键的名称 */
	private String sqlPrimaryId;
	/** 返回列 */
	private String sqlResultColumns;

	public AbstractStatementSQL(Class<?> entity) {
		super();
		Table table = entity.getAnnotation(Table.class);
		if (table == null || table.value().isEmpty()) {
			throw new NullPointerException(entity.getName() + " no Table annotation ,you need to set @Table on the class");
		}
		this.sqlTableName = this.getNameValue(table.value());
		boolean hasId = false;
		boolean hasCol = false;
		Field[] fields = entity.getDeclaredFields();
		StringBuilder column = new StringBuilder();
		for (Field field : fields) {
			field.setAccessible(true);
			TableId tableId = field.getAnnotation(TableId.class);
			TableColumn tableCol = field.getAnnotation(TableColumn.class);
			if (tableId == null && tableCol == null) {
				continue;
			}
			if (tableId != null) {
				if ( tableId.value().isEmpty()) {
					continue;
				}
				if (this.sqlPrimaryId != null) {
					this.sqlPrimaryId += this.getNameValue(tableId.value());
				} else {
					this.sqlPrimaryId = this.getNameValue(tableId.value());
				}
				hasId = true;
				column.append(",").append(this.getNameValue(tableId.value()));
				if (!tableId.alias().isEmpty()) {
					column.append(" AS ").append(this.getAliasNameValue(tableId.alias()));
				}
			} else {
				column.append(",").append(this.getNameValue(tableCol.value()));
				if (!tableCol.alias().isEmpty()) {
					column.append(" AS ").append(this.getAliasNameValue(tableCol.alias()));
				}
				hasCol = true;
			}
		}
		if (!hasId) {
			throw new NullPointerException(entity.getName() + " no TableId annotation ,you need to set @TableId on the field");
		}
		if (!hasCol && !hasId) {
			throw new NullPointerException(entity.getName() + " no TableColumn annotation ,you need to set @TableColumn on the field");
		}
		this.sqlResultColumns = column.substring(1);
	}

	protected String getAliasNameValue(String value) {
		return value;
	}

	protected String getNameValue(String value) {
		return value;
	}
	/**
	 * 获取表名称
	 * 
	 * @return
	 */
	public String getSqlTableName() {
		return sqlTableName;
	}

	/**
	 * 设置表名称
	 *
	 * @param sqlTableName
	 * @return
	 */
	public AbstractStatementSQL setSqlTableName(String sqlTableName) {
		this.sqlTableName = sqlTableName;
		return this;
	}

	/**
	 * 获取主键名称
	 * 
	 * @return
	 */
	public String getSqlPrimaryId() {
		return sqlPrimaryId;
	}

	/**
	 * 设置主键名称,当有多个主键时可以重写给方法设置以哪一个主键为主
	 * 
	 * @param sqlPrimaryId
	 * @return
	 */
	public AbstractStatementSQL setSqlPrimaryId(String sqlPrimaryId) {
		this.sqlPrimaryId = sqlPrimaryId;
		return this;
	}

	/**
	 * 获取表返回列
	 * 
	 * @return
	 */
	public String getSqlResultColumns() {
		return sqlResultColumns;
	}

	/**
	 * 设置表返回列
	 * 
	 * @param sqlResultColumns
	 * @return
	 */
	public AbstractStatementSQL setSqlResultColumns(String sqlResultColumns) {
		this.sqlResultColumns = sqlResultColumns;
		return this;
	}

	/**
	 * 表的所有列名与列名对应的值
	 * 
	 * @return
	 * @throws Exception
	 */
	protected <T> List<SqlPropertyValue<?>> getPropertyValue(T obj) throws Exception {
		Field[] fields = obj.getClass().getDeclaredFields();
		List<SqlPropertyValue<?>> result = new ArrayList<>();;
		for (Field field : fields) {
			field.setAccessible(true);
			TableId tableId = field.getAnnotation(TableId.class);
			TableColumn tableCol = field.getAnnotation(TableColumn.class);
			if (tableId == null && tableCol == null) {
				continue;
			}
			if (tableId != null) {
				result.add(0, new SqlPropertyValue<>(this.getNameValue(tableId.value()), field.get(obj)));
			} else {
				result.add(new SqlPropertyValue<>(this.getNameValue(tableCol.value()), field.get(obj)));
			}
		}
		return result;
	}

	@Override
	public SqlAndParams getCountSQL(SqlAssist assist) {
		StringBuilder sql = new StringBuilder(String.format("select count(*) from %s ", getSqlTableName()));
		JsonArray params = null;
		if (assist != null) {
			if (assist.getJoinOrReference() != null) {
				sql.append(assist.getJoinOrReference());
			}
			if (assist.getCondition() != null && assist.getCondition().size() > 0) {
				List<SqlWhereCondition<?>> where = assist.getCondition();
				params = new JsonArray();
				sql.append(" where ").append(where.get(0).getRequire());
				if (where.get(0).getValue() != null) {
					params.add(where.get(0).getValue());
				}
				if (where.get(0).getValues() != null) {
					for (Object value : where.get(0).getValues()) {
						params.add(value);
					}
				}
				for (int i = 1; i < where.size(); i++) {
					sql.append(where.get(i).getRequire());
					if (where.get(i).getValue() != null) {
						params.add(where.get(i).getValue());
					}
					if (where.get(i).getValues() != null) {
						for (Object value : where.get(i).getValues()) {
							params.add(value);
						}
					}
				}
			}
			if (assist.getGroupBy() != null) {
				sql.append(" group by ").append(assist.getGroupBy()).append(" ");
			}
		}
		SqlAndParams result = new SqlAndParams(sql.toString(), params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("getCountSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public SqlAndParams selectAllSQL(SqlAssist assist) {
		// 如果Assist为空返回默认默认查询语句,反则根据Assist生成语句sql语句
		if (assist == null) {
			SqlAndParams result = new SqlAndParams(String.format("select %s from %s ", getSqlResultColumns(), getSqlTableName()));
			if (this.getLOG().isDebugEnabled()) {
				this.getLOG().debug("SelectAllSQL : " + result.toString());
			}
			return result;
		} else {
			String distinct = assist.getDistinct() == null ? "" : assist.getDistinct();// 去重语句
			String column = assist.getResultColumn() == null ? getSqlResultColumns() : assist.getResultColumn();// 表的列名
			// 初始化SQL语句
			StringBuilder sql = new StringBuilder(String.format("select %s %s from %s", distinct, column, getSqlTableName()));
			JsonArray params = null;// 参数
			if (assist.getJoinOrReference() != null) {
				sql.append(assist.getJoinOrReference());
			}
			if (assist.getCondition() != null && assist.getCondition().size() > 0) {
				List<SqlWhereCondition<?>> where = assist.getCondition();
				params = new JsonArray();
				sql.append(" where ").append(where.get(0).getRequire());
				if (where.get(0).getValue() != null) {
					params.add(where.get(0).getValue());
				}
				if (where.get(0).getValues() != null) {
					for (Object value : where.get(0).getValues()) {
						params.add(value);
					}
				}
				for (int i = 1; i < where.size(); i++) {
					sql.append(where.get(i).getRequire());
					if (where.get(i).getValue() != null) {
						params.add(where.get(i).getValue());
					}
					if (where.get(i).getValues() != null) {
						for (Object value : where.get(i).getValues()) {
							params.add(value);
						}
					}
				}
			}
			if (assist.getGroupBy() != null) {
				sql.append(" group by ").append(assist.getGroupBy()).append(" ");
			}
			if (assist.getHaving() != null) {
				sql.append(" having ").append(assist.getHaving()).append(" ");
				if (assist.getHavingValue() != null) {
					if (params == null) {
						params = new JsonArray();
					}
					params.addAll(assist.getHavingValue());
				}
			}
			if (assist.getOrder() != null) {
				sql.append(assist.getOrder());
			}
			if (assist.getRowSize() != null || assist.getStartRow() != null) {
				if (params == null) {
					params = new JsonArray();
				}
				if (assist.getStartRow() != null) {
					sql.append(" LIMIT ?");
					params.add(assist.getRowSize());
				}
				if (assist.getStartRow() != null) {
					sql.append(" OFFSET ?");
					params.add(assist.getStartRow());
				}
			}
			SqlAndParams result = new SqlAndParams(sql.toString(), params);
			if (this.getLOG().isDebugEnabled()) {
				this.getLOG().debug("SelectAllSQL : " + result.toString());
			}
			return result;
		}
	}

	@Override
	public <S> SqlAndParams selectByIdSQL(S primaryValue, String resultColumns, String joinOrReference) {
		String sql = String.format("select %s from %s %s where %s = ? ", (resultColumns == null ? getSqlResultColumns() : resultColumns),
				getSqlTableName(), (joinOrReference == null ? "" : joinOrReference), getSqlPrimaryId());
		JsonArray params = new JsonArray();
		params.add(primaryValue);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("selectByIdSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams selectByObjSQL(T obj, String resultColumns, String joinOrReference, boolean single) {
		StringBuilder sql = new StringBuilder(
				String.format("select %s from %s %s ", (resultColumns == null ? getSqlResultColumns() : resultColumns), getSqlTableName(),
						(joinOrReference == null ? "" : joinOrReference)));
		JsonArray params = null;
		boolean isFrist = true;
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (int i = propertyValue.size() - 1; i >= 0; i--) {
			SqlPropertyValue<?> pv = propertyValue.get(i);
			if (pv.getValue() != null) {
				if (isFrist) {
					params = new JsonArray();
					sql.append(String.format("where %s = ? ", pv.getName()));
					params.add(pv.getValue());
					isFrist = false;
				} else {
					sql.append(String.format("and %s = ? ", pv.getName()));
					params.add(pv.getValue());
				}
			}
		}
		if (single) {
			sql.append(" LIMIT 1");
		}
		SqlAndParams result = new SqlAndParams(sql.toString(), params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("selectByObjSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams insertAllSQL(T obj) {
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
		String sql = String.format("insert into %s (%s) values (%s) ", getSqlTableName(), tempColumn, tempValues);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("insertAllSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams insertNonEmptySQL(T obj) {
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
		String sql = String.format("insert into %s (%s) values (%s) ", getSqlTableName(), tempColumn, tempValues);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("insertNonEmptySQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams replaceSQL(T obj) {
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
		String sql = String.format("replace into %s (%s) values (%s) ", getSqlTableName(), tempColumn, tempValues);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("replaceSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams updateAllByIdSQL(T obj) {
		if (getSqlPrimaryId() == null) {
			return new SqlAndParams(false, "there is no primary key in your SQL statement");
		}
		JsonArray params = new JsonArray();
		StringBuilder tempColumn = null;
		Object tempIdValue = null;
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (pv.getName().equals(getSqlPrimaryId())) {
				tempIdValue = pv.getValue();
				continue;
			}
			if (tempColumn == null) {
				tempColumn = new StringBuilder(pv.getName()).append(" = ? ");
			} else {
				tempColumn.append(", ").append(pv.getName()).append(" = ? ");
			}
			if (pv.getValue() != null) {
				params.add(pv.getValue());
			} else {
				params.addNull();
			}
		}
		if (tempIdValue == null) {
			return new SqlAndParams(false, "there is no primary key in your SQL statement");
		}
		params.add(tempIdValue);
		String sql = String.format("update %s set %s where %s = ? ", getSqlTableName(), tempColumn, getSqlPrimaryId());
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("updateAllByIdSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams updateAllByAssistSQL(T obj, SqlAssist assist) {
		if (assist == null || assist.getCondition() == null || assist.getCondition().size() < 1) {
			return new SqlAndParams(false, "SqlAssist or SqlAssist.condition is null");
		}
		JsonArray params = new JsonArray();
		StringBuilder tempColumn = null;
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (tempColumn == null) {
				tempColumn = new StringBuilder(pv.getName()).append(" = ? ");
			} else {
				tempColumn.append(", ").append(pv.getName()).append(" = ? ");
			}
			if (pv.getValue() != null) {
				params.add(pv.getValue());
			} else {
				params.addNull();
			}
		}
		List<SqlWhereCondition<?>> where = assist.getCondition();
		StringBuilder whereStr = new StringBuilder(" where " + where.get(0).getRequire());
		if (where.get(0).getValue() != null) {
			params.add(where.get(0).getValue());
		}
		if (where.get(0).getValues() != null) {
			for (Object value : where.get(0).getValues()) {
				params.add(value);
			}
		}
		for (int i = 1; i < where.size(); i++) {
			whereStr.append(where.get(i).getRequire());
			if (where.get(i).getValue() != null) {
				params.add(where.get(i).getValue());
			}
			if (where.get(i).getValues() != null) {
				for (Object value : where.get(i).getValues()) {
					params.add(value);
				}
			}
		}
		String sql = String.format("update %s set %s %s", getSqlTableName(), tempColumn, whereStr);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("updateAllByAssistSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams updateNonEmptyByIdSQL(T obj) {
		if (getSqlPrimaryId() == null) {
			if (this.getLOG().isDebugEnabled()) {
				this.getLOG().debug("there is no primary key in your SQL statement");
			}
			return new SqlAndParams(false, "there is no primary key in your SQL statement");
		}
		JsonArray params = null;
		StringBuilder tempColumn = null;
		Object tempIdValue = null;
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (pv.getName().equals(getSqlPrimaryId())) {
				tempIdValue = pv.getValue();
				continue;
			}
			if (pv.getValue() != null) {
				if (tempColumn == null) {
					params = new JsonArray();
					tempColumn = new StringBuilder(pv.getName()).append(" = ? ");
				} else {
					tempColumn.append(", ").append(pv.getName()).append(" = ? ");
				}
				params.add(pv.getValue());
			}
		}
		if (tempColumn == null || tempIdValue == null) {
			if (this.getLOG().isDebugEnabled()) {
				this.getLOG().debug("there is no set update value or no primary key in your SQL statement");
			}
			return new SqlAndParams(false, "there is no set update value or no primary key in your SQL statement");
		}
		params.add(tempIdValue);
		String sql = String.format("update %s set %s where %s = ? ", getSqlTableName(), tempColumn, getSqlPrimaryId());
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("updateNonEmptyByIdSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <T> SqlAndParams updateNonEmptyByAssistSQL(T obj, SqlAssist assist) {
		if (assist == null || assist.getCondition() == null || assist.getCondition().size() < 1) {
			return new SqlAndParams(false, "SqlAssist or SqlAssist.condition is null");
		}
		JsonArray params = null;
		StringBuilder tempColumn = null;
		List<SqlPropertyValue<?>> propertyValue;
		try {
			propertyValue = getPropertyValue(obj);
		} catch (Exception e) {
			return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
		}
		for (SqlPropertyValue<?> pv : propertyValue) {
			if (pv.getValue() != null) {
				if (tempColumn == null) {
					params = new JsonArray();
					tempColumn = new StringBuilder(pv.getName()).append(" = ? ");
				} else {
					tempColumn.append(", ").append(pv.getName()).append(" = ? ");
				}
				params.add(pv.getValue());
			}
		}
		if (tempColumn == null) {
			return new SqlAndParams(false, "The object has no value");
		}

		List<SqlWhereCondition<?>> where = assist.getCondition();
		StringBuilder whereStr = new StringBuilder(" where " + where.get(0).getRequire());
		if (where.get(0).getValue() != null) {
			params.add(where.get(0).getValue());
		}
		if (where.get(0).getValues() != null) {
			for (Object value : where.get(0).getValues()) {
				params.add(value);
			}
		}
		for (int i = 1; i < where.size(); i++) {
			whereStr.append(where.get(i).getRequire());
			if (where.get(i).getValue() != null) {
				params.add(where.get(i).getValue());
			}
			if (where.get(i).getValues() != null) {
				for (Object value : where.get(i).getValues()) {
					params.add(value);
				}
			}
		}
		String sql = String.format("update %s set %s %s", getSqlTableName(), tempColumn, whereStr);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("updateNonEmptyByAssistSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public <S> SqlAndParams updateSetNullByIdSQL(S primaryValue, List<String> columns) {
		if (getSqlPrimaryId() == null) {
			return new SqlAndParams(false, "there is no primary key in your SQL statement");
		}

		if (columns == null || columns.size() == 0) {
			return new SqlAndParams(false, "Columns cannot be null or empty");
		}
		StringBuilder setStr = new StringBuilder();
		setStr.append(" ").append(columns.get(0)).append(" = null ");
		for (int i = 1; i < columns.size(); i++) {
			setStr.append(", ").append(columns.get(i)).append(" = null ");
		}
		String sql = String.format("update %s set %s where %s = ? ", getSqlTableName(), setStr.toString(), getSqlPrimaryId());
		JsonArray params = new JsonArray();
		params.add(primaryValue);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("updateSetNullById : " + result.toString());
		}
		return result;
	}

	@Override
	public <S> SqlAndParams updateSetNullByAssistSQL(SqlAssist assist, List<String> columns) {
		if (assist == null || assist.getCondition() == null || assist.getCondition().size() < 1) {
			return new SqlAndParams(false, "SqlAssist or SqlAssist.condition is null");
		}
		if (columns == null || columns.size() == 0) {
			return new SqlAndParams(false, "Columns cannot be null or empty");
		}
		StringBuilder setStr = new StringBuilder();
		setStr.append(" ").append(columns.get(0)).append(" = null ");
		for (int i = 1; i < columns.size(); i++) {
			setStr.append(", ").append(columns.get(i)).append(" = null ");
		}
		JsonArray params = new JsonArray();
		List<SqlWhereCondition<?>> where = assist.getCondition();
		StringBuilder whereStr = new StringBuilder(" where " + where.get(0).getRequire());
		if (where.get(0).getValue() != null) {
			params.add(where.get(0).getValue());
		}
		if (where.get(0).getValues() != null) {
			for (Object value : where.get(0).getValues()) {
				params.add(value);
			}
		}
		for (int i = 1; i < where.size(); i++) {
			whereStr.append(where.get(i).getRequire());
			if (where.get(i).getValue() != null) {
				params.add(where.get(i).getValue());
			}
			if (where.get(i).getValues() != null) {
				for (Object value : where.get(i).getValues()) {
					params.add(value);
				}
			}
		}
		String sql = String.format("update %s set %s %s", getSqlTableName(), setStr.toString(), whereStr);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("updateSetNullByAssist : " + result.toString());
		}
		return result;
	}

	@Override
	public <S> SqlAndParams deleteByIdSQL(S primaryValue) {
		if (getSqlPrimaryId() == null) {
			return new SqlAndParams(false, "there is no primary key in your SQL statement");
		}
		String sql = String.format("delete from %s where %s = ? ", getSqlTableName(), getSqlPrimaryId());
		JsonArray params = new JsonArray();
		params.add(primaryValue);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("deleteByIdSQL : " + result.toString());
		}
		return result;
	}

	@Override
	public SqlAndParams deleteByAssistSQL(SqlAssist assist) {
		if (assist == null || assist.getCondition() == null || assist.getCondition().size() < 1) {
			return new SqlAndParams(false, "SqlAssist or SqlAssist.condition is null");
		}
		List<SqlWhereCondition<?>> where = assist.getCondition();
		JsonArray params = new JsonArray();
		StringBuilder whereStr = new StringBuilder(" where " + where.get(0).getRequire());
		if (where.get(0).getValue() != null) {
			params.add(where.get(0).getValue());
		}
		if (where.get(0).getValues() != null) {
			for (Object value : where.get(0).getValues()) {
				params.add(value);
			}
		}
		for (int i = 1; i < where.size(); i++) {
			whereStr.append(where.get(i).getRequire());
			if (where.get(i).getValue() != null) {
				params.add(where.get(i).getValue());
			}
			if (where.get(i).getValues() != null) {
				for (Object value : where.get(i).getValues()) {
					params.add(value);
				}
			}
		}
		String sql = String.format("delete from %s %s", getSqlTableName(), whereStr);
		SqlAndParams result = new SqlAndParams(sql, params);
		if (this.getLOG().isDebugEnabled()) {
			this.getLOG().debug("deleteByAssistSQL : " + result.toString());
		}
		return result;
	}

	protected abstract Logger getLOG();
}
