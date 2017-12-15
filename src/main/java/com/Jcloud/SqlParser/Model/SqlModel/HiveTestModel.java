package com.Jcloud.SqlParser.Model.SqlModel;

public class HiveTestModel {
    private Integer id;

    private String devSql;

    private String devSqlAuto;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getDevSql() {
        return devSql;
    }

    public void setDevSql(String devSql) {
        this.devSql = devSql == null ? null : devSql.trim();
    }

    public String getDevSqlAuto() {
        return devSqlAuto;
    }

    public void setDevSqlAuto(String devSqlAuto) {
        this.devSqlAuto = devSqlAuto == null ? null : devSqlAuto.trim();
    }
}