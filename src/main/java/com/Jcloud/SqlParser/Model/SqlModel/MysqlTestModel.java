package com.Jcloud.SqlParser.Model.SqlModel;

public class MysqlTestModel {
    private Integer id;

    private String interfaceSql;

    private String mysqlAutoSql;

    private String kylinAutoSql;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getInterfaceSql() {
        return interfaceSql;
    }

    public void setInterfaceSql(String interfaceSql) {
        this.interfaceSql = interfaceSql == null ? null : interfaceSql.trim();
    }

    public String getMysqlAutoSql() {
        return mysqlAutoSql;
    }

    public void setMysqlAutoSql(String mysqlAutoSql) {
        this.mysqlAutoSql = mysqlAutoSql == null ? null : mysqlAutoSql.trim();
    }

    public String getKylinAutoSql() {
        return kylinAutoSql;
    }

    public void setKylinAutoSql(String kylinAutoSql) {
        this.kylinAutoSql = kylinAutoSql == null ? null : kylinAutoSql.trim();
    }
}