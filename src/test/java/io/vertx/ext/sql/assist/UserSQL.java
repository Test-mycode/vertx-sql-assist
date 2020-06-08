package io.vertx.ext.sql.assist;

import io.vertx.ext.sql.SQLClient;

public class UserSQL extends CommonSQL<User, SQLClient> {
	public UserSQL(SQLExecute<SQLClient> execute) {
		super(execute);
	}
}
