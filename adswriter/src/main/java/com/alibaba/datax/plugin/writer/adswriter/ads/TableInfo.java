package com.alibaba.datax.plugin.writer.adswriter.ads;

import java.util.List;

/**
 * ADS table meta.<br>
 * <p>
 * select table_schema, table_name,comments <br>
 * from information_schema.tables <br>
 * where table_schema='alimama' and table_name='click_af' limit 1 <br>
 * </p>
 * <p>
 * select ordinal_position,column_name,data_type,type_name,column_comment <br>
 * from information_schema.columns <br>
 * where table_schema='db_name' and table_name='table_name' <br>
 * and is_deleted=0 <br>
 * order by ordinal_position limit 1000 <br>
 * </p>
 *
 * @since 0.0.1
 */
public class TableInfo {

    private String tableSchema;
    private String tableName;
    private List<ColumnInfo> columns;
    private String comments;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("TableInfo [tableSchema=").append(tableSchema).append(", tableName=").append(tableName)
                .append(", columns=").append(columns).append(", comments=").append(comments).append("]");
        return builder.toString();
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnInfo> columns) {
        this.columns = columns;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

}
