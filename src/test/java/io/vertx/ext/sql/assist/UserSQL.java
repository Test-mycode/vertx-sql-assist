package io.vertx.ext.sql.assist;

import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLOperations;
import io.vertx.ext.sql.assist.core.CommonSQL;
import io.vertx.ext.sql.assist.core.SQLExecute;

public class UserSQL extends CommonSQL<User, SQLOperations> {
	public UserSQL(SQLExecute<SQLOperations> execute) {
		super(execute);
	}
}
