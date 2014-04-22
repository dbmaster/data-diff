package io.dbmaster.tools.diff;


class ColumnMapperInfo {
    String name;
    ColumnInfo columnA;
    ColumnInfo columnB;

    @Override
    public String toString() {
        return columnA + "|" + columnB;
    }
}
