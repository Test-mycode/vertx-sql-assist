package io.vertx.ext.sql.assist;

import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * 数据库命令执行器的默认实现
 *
 * @author <a href="http://szmirren.com">Mirren</a>
 */
public class SQLCommandImpl implements SQLCommand {
    /**
     * 语句
     */
    private SQLStatement statement;
    /**
     * 执行器
     */
    private SQLExecute<?> execute;

    /**
     * 初始化
     *
     * @param statement
     * @param execute
     */
    public SQLCommandImpl(SQLStatement statement, SQLExecute<?> execute) {
        super();
        this.statement = statement;
        this.execute = execute;
    }

    @Override
    public Future<Long> getCount(SqlAssist assist) {
        SqlAndParams qp = statement.getCountSQL(assist);

        return execute.queryAsListArray(qp).compose(rows -> {
            if (rows != null && !rows.isEmpty()) {
                Object value = rows.get(0).getValue(0);
                if (value instanceof Number) {
                    return Future.succeededFuture(((Number) value).longValue());
                } else {
                    return Future.succeededFuture(0L);
                }
            } else {
                return Future.succeededFuture(0L);
            }
        });
    }

    @Override
    public Future<List<JsonObject>> selectAll(SqlAssist assist) {
        SqlAndParams qp = statement.selectAllSQL(assist);
        return execute.queryAsListObj(qp);
    }

    @Override
    public <S> Future<JsonObject> selectById(S primaryValue, String resultColumns, String joinOrReference) {
        SqlAndParams qp = statement.selectByIdSQL(primaryValue, resultColumns, joinOrReference);
        return execute.queryAsObj(qp);
    }

    @Override
    public <T> Future<JsonObject> selectSingleByObj(T obj, String resultColumns, String joinOrReference) {
        SqlAndParams qp = statement.selectByObjSQL(obj, resultColumns, joinOrReference, true);
        return execute.queryAsObj(qp);
    }

    @Override
    public <T> Future<List<JsonObject>> selectByObj(T obj, String resultColumns, String joinOrReference) {
        SqlAndParams qp = statement.selectByObjSQL(obj, resultColumns, joinOrReference, false);
        return execute.queryAsListObj(qp);
    }

    @Override
    public <T> Future<Integer> insertAll(T obj) {
        SqlAndParams qp = statement.insertAllSQL(obj);
        return execute.update(qp);
    }

	@Override
	public <T> Future<Integer> upsertAll(T obj) {
		SqlAndParams qp = statement.upsertAllSQL(obj);
		return execute.update(qp);
	}

	@Override
    public <T> Future<JsonArray> insertAllReturnId(T obj) {
        SqlAndParams qp = statement.insertAllSQLReturnId(obj);
        return execute.insert(qp);
    }

	@Override
	public <T> Future<JsonArray> upsertAllReturnId(T obj) {
		SqlAndParams qp = statement.upsertAllSQLReturnId(obj);
		return execute.insert(qp);
	}

	@Override
    public <T> Future<Integer> insertNonEmpty(T obj) {
        SqlAndParams qp = statement.insertNonEmptySQL(obj);
        return execute.update(qp);
    }

	@Override
	public <T> Future<Integer> upsertNonEmpty(T obj) {
		SqlAndParams qp = statement.upsertNonEmptySQL(obj);
		return execute.update(qp);
	}

	@Override
    public <T> Future<JsonArray> insertNonEmptyReturnId(T obj) {
        SqlAndParams qp = statement.insertNonEmptySQLReturnId(obj);
        return execute.insert(qp);
    }

	@Override
	public <T> Future<JsonArray> upsertNonEmptyReturnId(T obj) {
		SqlAndParams qp = statement.upsertNonEmptySQLReturnId(obj);
		return execute.insert(qp);
	}


	@Override
    public <T> Future<Long> insertBatch(List<T> list) {
        SqlAndParams qp = statement.insertBatchSQL(list);
        if (qp.succeeded()) {
            return execute.batch(qp)
                    .map(rows -> (long) rows.size());
        } else {
            return Future.succeededFuture(0L);
        }
    }

    @Override
    public Future<Long> insertBatch(List<String> columns, List<JsonArray> params) {
        SqlAndParams qp = statement.insertBatchSQL(columns, params);
        if (qp.succeeded()) {
            return execute.batch(qp).map(rows -> (long) rows.size());
        } else {
            return Future.succeededFuture(0L);
        }
    }

    @Override
    public <T> Future<Integer> replace(T obj) {
        SqlAndParams qp = statement.replaceSQL(obj);
        return execute.update(qp);
    }

    @Override
    public <T> Future<Integer> updateAllById(T obj) {
        SqlAndParams qp = statement.updateAllByIdSQL(obj);
        return execute.update(qp);
    }

    @Override
    public <T> Future<Integer> updateAllByAssist(T obj, SqlAssist assist) {
        SqlAndParams qp = statement.updateAllByAssistSQL(obj, assist);
        return execute.update(qp);
    }

    @Override
    public <T> Future<Integer> updateNonEmptyById(T obj) {
        SqlAndParams qp = statement.updateNonEmptyByIdSQL(obj);
        return execute.update(qp);
    }

    @Override
    public <T> Future<Integer> updateNonEmptyByAssist(T obj, SqlAssist assist) {
        SqlAndParams qp = statement.updateNonEmptyByAssistSQL(obj, assist);
        return execute.update(qp);
    }

    @Override
    public <S> Future<Integer> updateSetNullById(S primaryValue, List<String> columns) {
        SqlAndParams qp = statement.updateSetNullByIdSQL(primaryValue, columns);
        return execute.update(qp);
    }

    @Override
    public <T> Future<Integer> updateSetNullByAssist(SqlAssist assist, List<String> columns) {
        SqlAndParams qp = statement.updateSetNullByAssistSQL(assist, columns);
        return execute.update(qp);
    }

    @Override
    public <S> Future<Integer> deleteById(S primaryValue) {
        SqlAndParams qp = statement.deleteByIdSQL(primaryValue);
        return execute.update(qp);
    }

    @Override
    public Future<Integer> deleteByAssist(SqlAssist assist) {
        SqlAndParams qp = statement.deleteByAssistSQL(assist);
        return execute.update(qp);
    }

}
