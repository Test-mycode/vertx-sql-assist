package io.vertx.ext.sql.assist.sql;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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
    /*
    * 表列的缓存
    * */
    private final ConcurrentHashMap<Class<?>,Field[]> tableCache;
    /**
     * 表的名称
     */
    protected String sqlTableName;
    /**
     * 主键的名称
     */
	protected String sqlPrimaryId;
    /**
     * 返回列
     */
	protected String sqlResultColumns;

    public AbstractStatementSQL(Class<?> entity) {
        this.tableCache= new ConcurrentHashMap<>();
        this.parseTable(entity);
        this.parseColumn(entity);
    }

    private void parseTable(Class<?> entity) {
        Table table = entity.getAnnotation(Table.class);
        if (table == null || table.value().isEmpty()) {
            throw new NullPointerException(entity.getName() + " no Table annotation ,you need to set @Table on the class");
        }
        this.sqlTableName = this.getNameValue(table.value());
    }

    private void parseColumn(Class<?> entity) {
        LinkedList<String> column = new LinkedList<>();
        for (Field field : entity.getDeclaredFields()) {
            field.setAccessible(true);
            TableId tableId = field.getAnnotation(TableId.class);
            TableColumn tableCol = field.getAnnotation(TableColumn.class);

            if (tableId != null) {
                if (tableId.value().isEmpty())
                    continue;

                if(tableId.alias().isEmpty())
                	column.add(this.getNameValue(tableId.value()));
                else
                	column.add(this.getNameValue(tableId.value())+" AS "+this.getAliasNameValue(tableId.alias()));

                if(this.sqlPrimaryId!=null)
                	continue;

                this.sqlPrimaryId = this.getNameValue(tableId.value());
            }
            if (tableCol != null) {
				if (tableCol.value().isEmpty())
					continue;

				if(tableCol.alias().isEmpty())
					column.add(this.getNameValue(tableCol.value()));
				else
					column.add(this.getNameValue(tableCol.value())+" AS "+this.getAliasNameValue(tableCol.alias()));
            }
        }

        if (this.sqlPrimaryId == null) {
            throw new NullPointerException(entity.getName() + " no TableId annotation ,you need to set @TableId on the field");
        }

        if (column.isEmpty()) {
            throw new NullPointerException(entity.getName() + " no TableColumn annotation ,you need to set @TableColumn on the field");
        }

        this.sqlResultColumns = String.join(",",column);
    }

    /**
     * 获取别名 name to `name`
     *
     * @return String
     */
    protected abstract String getAliasNameValue(String value);

    /**
     * 获取表名、列名 name to `name`
     *
     * @return String
     */
    protected abstract String getNameValue(String value);


    /**
     * 表的所有列名与列名对应的值
     *
     * @return List
     */
    protected <T> List<SqlPropertyValue<?>> getPropertyValue(T obj) throws Exception {
        List<SqlPropertyValue<?>> result = new LinkedList<>();
        Field [] fields = this.tableCache.computeIfAbsent(obj.getClass(),cls->obj.getClass().getDeclaredFields());
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

	public void parseSqlAssist(SqlAssist assist,StringBuilder stringBuffer,JsonArray params,Boolean withPage) {
		if (assist.getJoinOrReference() != null) {
			stringBuffer.append(assist.getJoinOrReference());
		}
		if (assist.getCondition() != null && assist.getCondition().size() > 0) {
			List<SqlWhereCondition<?>> where = assist.getCondition();
			stringBuffer.append(" where ").append(where.get(0).getRequire());
			if (where.get(0).getValue() != null) {
				params.add(where.get(0).getValue());
			}
			if (where.get(0).getValues() != null) {
				for (Object value : where.get(0).getValues()) {
					params.add(value);
				}
			}
			for (int i = 1; i < where.size(); i++) {
				stringBuffer.append(where.get(i).getRequire());
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
            stringBuffer.append(" group by ").append(assist.getGroupBy()).append(" ");
        }
        if (assist.getHaving() != null) {
            stringBuffer.append(" having ").append(assist.getHaving()).append(" ");
            if (assist.getHavingValue() != null) {
                if (params == null) {
                    params = new JsonArray();
                }
                params.addAll(assist.getHavingValue());
            }
        }
        if (assist.getOrder() != null) {
            stringBuffer.append(assist.getOrder());
        }
        if (withPage && (assist.getRowSize() != null || assist.getStartRow() != null )) {
            if (params == null) {
                params = new JsonArray();
            }
            if (assist.getStartRow() != null) {
                stringBuffer.append(" LIMIT ?");
                params.add(assist.getRowSize());
            }
            if (assist.getStartRow() != null) {
                stringBuffer.append(" OFFSET ?");
                params.add(assist.getStartRow());
            }
        }
	}

    @Override
    public SqlAndParams getCountSQL(SqlAssist assist) {
        StringBuilder sql = new StringBuilder(String.format("select count(0) from %s ", this.sqlTableName));
        JsonArray params = new JsonArray();
        if (assist != null) {
           this.parseSqlAssist(assist,sql,params,false);
        }
        SqlAndParams result = new SqlAndParams(sql.toString(), params);
        if (this.getLOG().isDebugEnabled()) {
            this.getLOG().debug("getCountSQL : " + result.toString());
        }
        return result;
    }

    @Override
    public SqlAndParams getExistSQL(SqlAssist assist) {
        StringBuilder sql = new StringBuilder(String.format("select 1 from %s ", this.sqlTableName));
        JsonArray params = new JsonArray();
        if (assist != null) {
            this.parseSqlAssist(assist,sql,params,false);
        }
        sql.append(" limit 1");
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
            SqlAndParams result = new SqlAndParams(String.format("select %s from %s ", this.sqlResultColumns, this.sqlTableName));
            if (this.getLOG().isDebugEnabled()) {
                this.getLOG().debug("SelectAllSQL : " + result.toString());
            }
            return result;
        } else {
            String distinct = assist.getDistinct() == null ? "" : assist.getDistinct();// 去重语句
            String column = assist.getResultColumn() == null ? this.sqlResultColumns : assist.getResultColumn();// 表的列名
            // 初始化SQL语句
            StringBuilder sql = new StringBuilder(String.format("select %s %s.%s from %s", distinct, this.sqlTableName, column, this.sqlTableName));
            JsonArray params = new JsonArray();// 参数
            this.parseSqlAssist(assist,sql,params,true);
            SqlAndParams result = new SqlAndParams(sql.toString(), params);
            if (this.getLOG().isDebugEnabled()) {
                this.getLOG().debug("SelectAllSQL : " + result.toString());
            }
            return result;
        }
    }

    @Override
    public <S> SqlAndParams selectByIdSQL(S primaryValue, String resultColumns, String joinOrReference) {
        String sql = String.format("select %s.%s from %s %s where %s = ? ", this.sqlTableName, (resultColumns == null ? this.sqlResultColumns : resultColumns),
                this.sqlTableName, (joinOrReference == null ? "" : joinOrReference), this.sqlPrimaryId);
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
                String.format("select %s.%s from %s %s ", this.sqlTableName, (resultColumns == null ? this.sqlResultColumns : resultColumns), this.sqlTableName,
                        (joinOrReference == null ? "" : joinOrReference)));
        JsonArray params = null;
        boolean first = true;
        List<SqlPropertyValue<?>> propertyValue;
        try {
            propertyValue = getPropertyValue(obj);
        } catch (Exception e) {
            return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
        }
        for (int i = propertyValue.size() - 1; i >= 0; i--) {
            SqlPropertyValue<?> pv = propertyValue.get(i);
            if (pv.getValue() != null) {
                if (first) {
                    params = new JsonArray();
                    sql.append(String.format("where %s = ? ", pv.getName()));
                    params.add(pv.getValue());
                    first = false;
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
            tempColumn.add(pv.getName());
            tempValues.add("?");
            if (pv.getValue() != null) {
                params.add(pv.getValue());
            } else {
                params.addNull();
            }
        }
        String sql = String.format("insert into %s (%s) values (%s) ", this.sqlTableName, String.join(",",tempColumn), String.join(",",tempValues));
        SqlAndParams result = new SqlAndParams(sql, params);
        if (this.getLOG().isDebugEnabled()) {
            this.getLOG().debug("insertAllSQL : " + result.toString());
        }
        return result;
    }

    @Override
    public <T> SqlAndParams insertNonEmptySQL(T obj) {
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
        ;
        String sql = String.format("insert into %s (%s) values (%s) ", this.sqlTableName,String.join(",",tempColumn) , String.join(",",tempValues));
        SqlAndParams result = new SqlAndParams(sql, params);
        if (this.getLOG().isDebugEnabled()) {
            this.getLOG().debug("insertNonEmptySQL : " + result.toString());
        }
        return result;
    }

    @Override
    public <T> SqlAndParams updateAllByIdSQL(T obj) {
        if (this.sqlPrimaryId == null) {
            return new SqlAndParams(false, "there is no primary key in your SQL statement");
        }
        JsonArray params = new JsonArray();
        LinkedList<String> tempColumn = new LinkedList<>();
        Object tempIdValue = null;
        List<SqlPropertyValue<?>> propertyValue;
        try {
            propertyValue = getPropertyValue(obj);
        } catch (Exception e) {
            return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
        }
        for (SqlPropertyValue<?> pv : propertyValue) {
            if (pv.getName().equals(this.sqlPrimaryId)) {
                tempIdValue = pv.getValue();
                continue;
            }
            tempColumn.add(pv.getName()+" = ? ");
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
        String sql = String.format("update %s set %s where %s = ? ", this.sqlTableName, String.join(",",tempColumn), this.sqlPrimaryId);
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
		LinkedList<String> tempColumn = new LinkedList<>();
        List<SqlPropertyValue<?>> propertyValue;
        try {
            propertyValue = getPropertyValue(obj);
        } catch (Exception e) {
            return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
        }
        for (SqlPropertyValue<?> pv : propertyValue) {
			tempColumn.add(pv.getName()+" = ? ");
            if (pv.getValue() != null) {
                params.add(pv.getValue());
            } else {
                params.addNull();
            }
        }

        StringBuilder whereStr = new StringBuilder();
        this.parseSqlAssist(assist,whereStr,params,false);

        String sql = String.format("update %s set %s %s", this.sqlTableName, String.join(",",tempColumn), whereStr);

        SqlAndParams result = new SqlAndParams(sql, params);

        if (this.getLOG().isDebugEnabled()) {
            this.getLOG().debug("updateAllByAssistSQL : " + result.toString());
        }
        return result;
    }

    @Override
    public <T> SqlAndParams updateNonEmptyByIdSQL(T obj) {
        if (this.sqlPrimaryId == null) {
            if (this.getLOG().isDebugEnabled()) {
                this.getLOG().debug("there is no primary key in your SQL statement");
            }
            return new SqlAndParams(false, "there is no primary key in your SQL statement");
        }
        JsonArray params = new JsonArray();
        LinkedList<String> tempColumn = new LinkedList<>();
        Object tempIdValue = null;
        List<SqlPropertyValue<?>> propertyValue;
        try {
            propertyValue = getPropertyValue(obj);
        } catch (Exception e) {
            return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
        }
        for (SqlPropertyValue<?> pv : propertyValue) {
            if (pv.getName().equals(this.sqlPrimaryId)) {
                tempIdValue = pv.getValue();
                continue;
            }
            if (pv.getValue() != null) {
                tempColumn.add(pv.getName()+ " = ? ");
                params.add(pv.getValue());
            }
        }
        if (tempColumn.isEmpty() || tempIdValue == null) {
            if (this.getLOG().isDebugEnabled()) {
                this.getLOG().debug("there is no set update value or no primary key in your SQL statement");
            }
            return new SqlAndParams(false, "there is no set update value or no primary key in your SQL statement");
        }
        params.add(tempIdValue);
        String sql = String.format("update %s set %s where %s = ? ", this.sqlTableName, String.join(",",tempColumn), this.sqlPrimaryId);
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

        JsonArray params = new JsonArray();
        LinkedList<String> tempColumn = new LinkedList<>();
        List<SqlPropertyValue<?>> propertyValue;

        try {
            propertyValue = getPropertyValue(obj);
        } catch (Exception e) {
            return new SqlAndParams(false, " Get SqlPropertyValue failed: " + e.getMessage());
        }

        for (SqlPropertyValue<?> pv : propertyValue) {
            if (pv.getValue() != null) {
				tempColumn.add(pv.getName()+ " = ? ");
				params.add(pv.getValue());
            }
        }

        if (tempColumn.isEmpty()) {
            return new SqlAndParams(false, "The object has no value");
        }

        StringBuilder whereStr = new StringBuilder();

        this.parseSqlAssist(assist,whereStr,params,false);

        String sql = String.format("update %s set %s %s", this.sqlTableName, String.join(",",tempColumn), whereStr);

        SqlAndParams result = new SqlAndParams(sql, params);

        if (this.getLOG().isDebugEnabled()) {
            this.getLOG().debug("updateNonEmptyByAssistSQL : " + result.toString());
        }

        return result;
    }

    @Override
    public <S> SqlAndParams updateSetNullByIdSQL(S primaryValue, List<String> columns) {
        if (this.sqlPrimaryId == null) {
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

        String sql = String.format("update %s set %s where %s = ? ", this.sqlTableName, setStr.toString(), this.sqlPrimaryId);
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
        StringBuilder whereStr = new StringBuilder();
        this.parseSqlAssist(assist,whereStr,params,false);

        String sql = String.format("update %s set %s %s", this.sqlTableName, setStr.toString(), whereStr);
        SqlAndParams result = new SqlAndParams(sql, params);
        if (this.getLOG().isDebugEnabled()) {
            this.getLOG().debug("updateSetNullByAssist : " + result.toString());
        }
        return result;
    }

    @Override
    public <S> SqlAndParams deleteByIdSQL(S primaryValue) {
        if (this.sqlPrimaryId == null) {
            return new SqlAndParams(false, "there is no primary key in your SQL statement");
        }
        String sql = String.format("delete from %s where %s = ? ", this.sqlTableName, this.sqlPrimaryId);
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
        JsonArray params = new JsonArray();
        StringBuilder whereStr = new StringBuilder();
        this.parseSqlAssist(assist,whereStr,params,false);
        String sql = String.format("delete from %s %s", this.sqlTableName, whereStr);
        SqlAndParams result = new SqlAndParams(sql, params);
        if (this.getLOG().isDebugEnabled()) {
            this.getLOG().debug("deleteByAssistSQL : " + result.toString());
        }
        return result;
    }

    protected abstract Logger getLOG();
}
