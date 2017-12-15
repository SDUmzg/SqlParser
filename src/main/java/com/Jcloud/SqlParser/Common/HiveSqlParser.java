package com.Jcloud.SqlParser.Common;

import com.Jcloud.SqlParser.Model.SqlResult;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by mzg on 2017/12/13.
 */
@Component
public class HiveSqlParser {
    /**
     * 给Hive语句卖家表增加权限、格式化
     * @param sql   需要格式化的sql
     * @param type  数据库类型"hive"
     * @param appkey  确定ISV身份的appkey
     * @param tbType  是否是卖家表 1-是  2-否
     * @return
     */
    public SqlResult OdpsInsertParser(String sql,String type,String appkey,String tbType){
        SqlResult result = new SqlResult();
        String dbType = "";
        if (type.equals("hive")){
            dbType = JdbcConstants.ODPS;
        }else {
            result.setStatue(false);
            result.setValue("DataSource Error");
            return result;
        }
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        SQLStatement stmt = stmtList.get(0);
        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
        stmt.accept(statVisitor);

        String resultSql="";
        //是否是卖家表 1-是  2-否
        if (tbType.equals("1")){
            String changeTb = getTableNameByOperation(statVisitor,"Select");
            if (changeTb.trim().length()>0){
                resultSql = getAuthoritySQL(statVisitor,appkey,dbType)+getAfterWhereColumnsSql(stmtList,dbType);
                System.out.println(resultSql);
                result.setStatue(true);
                result.setValue(resultSql);
                return result;
            }

        }else if (tbType.equals("2")){
            result.setStatue(true);
            result.setValue(SQLUtils.toSQLString(stmtList,dbType));
        }else {
            result.setStatue(false);
            result.setValue("tbType Error");
        }
        System.out.println("-------------------------");
        return result;
    }

    public String getAuthoritySQL(SchemaStatVisitor statVisitor,String appkey,String changeTb){
        String insertSql="";
        String selectSql="";
        String insertTb = getTableNameByOperation(statVisitor,"Insert");
        String selectTb = getTableNameByOperation(statVisitor,"Select");
        insertSql = "INSERT OVERWRITE TABLE pri_result.dws_itm_platform_plot_trade_d  ";
        selectSql = getSelectColumnsSql(statVisitor) + getFromColumnsSql(statVisitor,appkey);
        return insertSql+selectSql;
    }

    public String getSelectColumnsSql(SchemaStatVisitor statVisitor){
        String result ="SELECT  ";
        Collection<TableStat.Column> columnList = statVisitor.getColumns();
        Iterator iterator = columnList.iterator();
        while (iterator.hasNext()){
            TableStat.Column column = (TableStat.Column)iterator.next();
            if (column.isSelect()){
                result += column.getFullName()+",";
            }

        }
        return result.substring(0,result.length()-1);
    }

    public String getFromColumnsSql(SchemaStatVisitor statVisitor,String appkey){
        String result = "  \nFROM ";
        String tb = getTableNameByOperation(statVisitor,"Select");
        String addTb = selectSqlSource.replace("{#table}",tb);
        String addAppKey = addTb.replace("{#appkey}",appkey);
        result+=addAppKey;
        return result;
    }

    public String getAfterWhereColumnsSql(List<SQLStatement> stmtList,String dbType){
        String sql =  SQLUtils.toSQLString(stmtList,dbType);
        String result = sql.substring(sql.indexOf("WHERE"),sql.length());
        return result;
    }

    public String getTableNameByOperation(SchemaStatVisitor statVisitor,String Operation){
        Map<TableStat.Name,TableStat> tableMap = statVisitor.getTables();
        Iterator<Map.Entry<TableStat.Name,TableStat>> entryIterator = tableMap.entrySet().iterator();
        while (entryIterator.hasNext()){
            Map.Entry<TableStat.Name,TableStat> entry = entryIterator.next();
            if (entry.getValue().toString().equals(Operation)){
                String changeTb =  entry.getKey().toString();
                return changeTb;
            }
        }
        return "";
    }

    public static final String insertSqlSource = " INSERT OVERWRITE TABLE {#table} ";

    public static final String selectSqlSource =" \n( SELECT {#table}.*\n" +
            "FROM sys.jddp_isv_seller\n" +
            "JOIN {#table}\n" +
            "ON jddp_isv_seller.seller_id = {#table}.seller_id\n" +
            "    AND jddp_isv_seller.appkey = '{#appkey}'\n" +
            "    AND jddp_isv_seller.enable_flag = '1' )\n";
}
