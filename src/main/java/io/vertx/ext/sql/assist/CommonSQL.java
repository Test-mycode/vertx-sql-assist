package io.vertx.ext.sql.assist;

import java.lang.reflect.ParameterizedType;
import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * 通用的数据库操作客户端的默认实现,
 * 
 * @author <a href="http://mirrentools.org">Mirren</a>
 *
 * @param <E>
 *          实体类的类型
 * @param <C>
 *          SQL执行器的客户端类型,比如JDBCClient
 */
public abstract class CommonSQL<E, C> implements CommonSQLClient<C> {
	/** SQL 执行器 */
	private SQLExecute<C> execute;
	/** SQL 命令 */
	private final SQLCommand command;

	/**
	 * 使用以注册或默认的{@link SQLStatement}
	 *          实体类,类必须包含{@link Table} {@link TableId} {@link TableColumn}注解
	 * @param execute
	 *          执行器
	 */
	public CommonSQL(SQLExecute<C> execute) {
		Class<?> entityClz = (Class<?>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
		SQLStatement statement = SQLStatement.create(entityClz);
		this.execute = execute;
		this.command = new SQLCommandImpl(statement, execute);
	}

	/**
	 * 使用自定义的{@link SQLStatement}
	 * 
	 * @param execute
	 *          SQL执行器
	 * @param statement
	 *          SQL执行语句
	 */
	public CommonSQL(SQLExecute<E> execute, SQLStatement statement) {
		this.command = new SQLCommandImpl(statement, execute);
	}

	/**
	 * 获取客户端
	 * 
	 * @return
	 * 
	 */
	@Override
	public C getDbClient() {
		return execute.getClient();
	}

	@Override
	public Future<Long> getCount() {
		return command.getCount();
	}

	@Override
	public Future<Long> getCount(SqlAssist assist) {
		return command.getCount(assist);
	}

	@Override
	public Future<List<JsonObject>> selectAll() {
		return command.selectAll();
	}

	@Override
	public Future<List<JsonObject>> selectAll(SqlAssist assist) {
		return command.selectAll(assist);
	}

	@Override
	public Future<JsonObject> limitAll(SqlAssist assist) {
		return command.limitAll(assist);
	}

	@Override
	public <S> Future<JsonObject> selectById(S primaryValue) {
		return command.selectById(primaryValue);
	}

	@Override
	public <S> Future<JsonObject> selectById(S primaryValue, String resultColumns) {
		return command.selectById(primaryValue, resultColumns);
	}

	@Override
	public <T> Future<JsonObject> selectSingleByObj(T obj) {
		return command.selectSingleByObj(obj);
	}

	@Override
	public <T> Future<JsonObject> selectSingleByObj(T obj, String resultColumns) {
		return command.selectSingleByObj(obj, resultColumns);
	}

	@Override
	public <T> Future<List<JsonObject>> selectByObj(T obj) {
		return command.selectByObj(obj);
	}

	@Override
	public <T> Future<List<JsonObject>> selectByObj(T obj, String resultColumns) {
		return command.selectByObj(obj, resultColumns);
	}

	@Override
	public <T> Future<Integer> insertAll(T obj) {
		return command.insertAll(obj);
	}

	@Override
	public <T> Future<Integer> upsertAll(T obj) {
		return command.upsertAll(obj);
	}
	@Override
	public <T> Future<Integer> insertNonEmpty(T obj) {
		return command.insertNonEmpty(obj);
	}

	@Override
	public <T> Future<Integer> upsertNonEmpty(T obj) {
		return command.upsertNonEmpty(obj);
	}

	@Override
	public <T> Future<JsonArray> insertNonEmptyReturnId(T obj) {
		return command.insertNonEmptyReturnId(obj);
	}

	@Override
	public <T> Future<Integer> replace(T obj) {
		return command.replace(obj);
	}

	@Override
	public <T> Future<Integer> updateAllById(T obj) {
		return command.updateAllById(obj);
	}

	@Override
	public <T> Future<Integer> updateAllByAssist(T obj, SqlAssist assist) {
		return command.updateAllByAssist(obj, assist);
	}

	@Override
	public <T> Future<Integer> updateNonEmptyById(T obj) {
		return command.updateNonEmptyById(obj);
	}

	@Override
	public <T> Future<Integer> updateNonEmptyByAssist(T obj, SqlAssist assist) {
		return command.updateNonEmptyByAssist(obj, assist);
	}

	@Override
	public <S> Future<Integer> updateSetNullById(S primaryValue, List<String> columns) {
		return command.updateSetNullById(primaryValue, columns);
	}

	@Override
	public <T> Future<Integer> updateSetNullByAssist(SqlAssist assist, List<String> columns) {
		return command.updateSetNullByAssist(assist, columns);
	}

	@Override
	public <S> Future<Integer> deleteById(S primaryValue) {
		return command.deleteById(primaryValue);
	}

	@Override
	public Future<Integer> deleteByAssist(SqlAssist assist) {
		return command.deleteByAssist(assist);
	}

}
