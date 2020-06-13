package io.vertx.ext.sql.assist;

import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.assist.core.CommonSQL;
import io.vertx.ext.sql.assist.core.SQLExecute;

public class UserSQL extends CommonSQL<User, SQLClient> {
	public UserSQL(SQLExecute<SQLClient> execute) {
		super(execute);
	}
}
