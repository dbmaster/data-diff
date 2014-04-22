package io.dbmaster.tools.diff;

import java.sql.ResultSet;
import java.sql.SQLException;

class ColumnInfo {
    String name;
    String typeName;
    String typeClass;
    int type;
    int rsIndex;

    Object value;

    @Override
    public String toString() {
        return name;
    }

    public void read(ResultSet rs) throws SQLException{
        value = rs.getObject(rsIndex);
    }
}
