package com.Jcloud.SqlParser.Common;

import com.Jcloud.SqlParser.Model.SqlResult;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.odps.ast.OdpsInsert;
import com.alibaba.druid.sql.dialect.odps.ast.OdpsInsertStatement;
import com.alibaba.druid.sql.dialect.odps.ast.OdpsSelectQueryBlock;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by mzg on 2017/12/13.
 */
public class HiveSqlParserTest {
    @Test
    public void odpsInsertParser() throws Exception {
        HiveSqlParser hiveSqlParser = new HiveSqlParser();
        SqlResult sqlResult = hiveSqlParser.OdpsInsertParser(sql4,"hive","1","1");
        hiveSqlParser.OdpsInsertParser(sqlResult.getValue(),"hive","1","1");
    }

    @Test
    public void temp0Test()throws Exception{
        String dbType = JdbcConstants.ODPS;
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql5, dbType);
        SQLStatement stmt = stmtList.get(0);
        SQLSelectStatement sqlSelectStatement= (SQLSelectStatement) stmt;
        SQLSelectQueryBlock sqlSelectQueryBlock = sqlSelectStatement.getSelect().getQueryBlock();
        SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) sqlSelectQueryBlock.getFrom();
        sqlJoinTableSource.getLeft();
        sqlJoinTableSource.getRight();
        SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlJoinTableSource.getRight();

        sqlJoinTableSource.getCondition();
        System.out.println(SQLUtils.format(sql5,dbType));
        System.out.println();
    }

    @Test
    public void tempTest() throws Exception{
        String dbType = JdbcConstants.ODPS;
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql3, dbType);
        SQLStatement stmt = stmtList.get(0);
        SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(dbType);
        stmt.accept(statVisitor);
        OdpsInsertStatement odstmt = (OdpsInsertStatement)stmt;
        OdpsInsert odpsInsert = odstmt.getItems().get(0);
        boolean isOverwrite = odpsInsert.isOverwrite();
        SQLExprTableSource tbSource = odpsInsert.getTableSource();
        SQLSelect sqlSelect = odpsInsert.getQuery();
        OdpsSelectQueryBlock odpsSelectQueryBlock = (OdpsSelectQueryBlock) sqlSelect.getQueryBlock();
        List<SQLAssignItem> sqlPartitions = odpsInsert.getPartitions();
        SQLTableSource from = odpsSelectQueryBlock.getFrom();
        setAuthOdpsSelectQueryBlock(odpsSelectQueryBlock,from,"appkey123456789");
        System.out.println(odpsSelectQueryBlock.toString());
        System.out.println();



    }

    public void setAuthOdpsSelectQueryBlock(OdpsSelectQueryBlock selectQueryBlock,SQLTableSource from,String appkey){
        if (from instanceof SQLJoinTableSource){
            SQLJoinTableSource joinTableSource = (SQLJoinTableSource) from;
            SQLTableSource joinLeft = joinTableSource.getLeft();
            setAuthOdpsSelectQueryBlock(selectQueryBlock,joinLeft,appkey);
            SQLTableSource joinRight = joinTableSource.getRight();
            setAuthOdpsSelectQueryBlock(selectQueryBlock,joinRight,appkey);
        }else if (from instanceof SQLSubqueryTableSource){
            SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) from;
            SQLSelect sqlSelect = sqlSubqueryTableSource.getSelect();
            SQLSelectQueryBlock sqlSelectQueryBlock = sqlSelect.getQueryBlock();
            SQLTableSource sqlTableSource = sqlSelectQueryBlock.getFrom();
            setAuthOdpsSelectQueryBlock(selectQueryBlock,sqlTableSource,appkey);
        }else if (from instanceof SQLExprTableSource){
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) from;
            SQLExpr sqlExpr =  sqlExprTableSource.getExpr();
            String sqlTbName = "";
            String sqlTbOwner = "";
            if (sqlExpr instanceof SQLPropertyExpr){
                SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlExpr;
                sqlTbName = sqlPropertyExpr.getName();
                sqlTbOwner = sqlPropertyExpr.getOwnernName();
            }else if (sqlExpr instanceof SQLIdentifierExpr){
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr)sqlExpr;
                sqlTbName = sqlIdentifierExpr.getName();
            }


            //构造过滤数据权限的语句
            OdpsSelectQueryBlock odpsSelectQueryBlock = new OdpsSelectQueryBlock();
            //构建Select语句
            SQLSelectItem sqlSelectItem = new SQLSelectItem();
            SQLPropertyExpr sqlPropertyExpr1 = new SQLPropertyExpr();
            sqlPropertyExpr1.setOwner(sqlTbName);
            sqlPropertyExpr1.setName("*");
            sqlSelectItem.setExpr(sqlPropertyExpr1);


            //构建From语句
            SQLJoinTableSource fromTableSource = new SQLJoinTableSource();
            SQLExprTableSource leftTableSource = new SQLExprTableSource();
            SQLPropertyExpr leftPropertyExpr = getSQLPropertyExpr("jddp_isv_seller","sys");
            leftTableSource.setExpr(leftPropertyExpr);
            SQLExprTableSource rightTableSource = new SQLExprTableSource();
            rightTableSource.setExpr(sqlExpr);
            SQLJoinTableSource.JoinType joinType = SQLJoinTableSource.JoinType.JOIN;

            //构建Join语句中Condition中     Condition
            SQLBinaryOpExpr sqlBinaryOpExprRoot = new SQLBinaryOpExpr();

            //构建Join语句中Condition中     Condition -> Right
            SQLBinaryOpExpr sqlBinaryOpExprRight = new SQLBinaryOpExpr();
            SQLPropertyExpr sqlPropertyExprRightLeft = getSQLPropertyExpr("enable_flag","jddp_isv_seller");
            SQLCharExpr sqlCharExprRightRight = new SQLCharExpr();
            sqlCharExprRightRight.setText("1");

            //构建Join语句中Condition中     Condition -> Right  组合
            sqlBinaryOpExprRight.setLeft(sqlPropertyExprRightLeft);
            sqlBinaryOpExprRight.setOperator(SQLBinaryOperator.Equality);
            sqlBinaryOpExprRight.setRight(sqlCharExprRightRight);

            // 构建Join语句中Condition中     Condition -> Left
            SQLBinaryOpExpr sqlBinaryOpExprLeft = new SQLBinaryOpExpr();
            // Condition -> Left -> Right
            SQLBinaryOpExpr sqlBinaryOpExprLeftRight = new SQLBinaryOpExpr();
            SQLPropertyExpr sqlPropertyExprLeftRightLeft = getSQLPropertyExpr("appkey","jddp_isv_seller");
            SQLCharExpr sqlCharExprLeftRightRight = new SQLCharExpr();
            sqlCharExprLeftRightRight.setText(appkey);
            sqlBinaryOpExprLeftRight.setLeft(sqlPropertyExprLeftRightLeft);
            sqlBinaryOpExprLeftRight.setOperator(SQLBinaryOperator.Equality);
            sqlBinaryOpExprLeftRight.setRight(sqlCharExprLeftRightRight);

            //Condition -> Left -> Left
            SQLBinaryOpExpr sqlBinaryOpExprLeftLeft = new SQLBinaryOpExpr();
            SQLPropertyExpr sqlPropertyExprLeftLeftLeft = getSQLPropertyExpr("seller_id","jddp_isv_seller");
            SQLPropertyExpr sqlPropertyExprLeftLeftRight = getSQLPropertyExpr("seller_id",sqlTbName);

            //Condition -> Left -> Left 组合
            sqlBinaryOpExprLeftLeft.setLeft(sqlPropertyExprLeftLeftLeft);
            sqlBinaryOpExprLeftLeft.setOperator(SQLBinaryOperator.Equality);
            sqlBinaryOpExprLeftLeft.setRight(sqlPropertyExprLeftLeftRight);


            // 构建Join语句中Condition中     Condition -> Left  组合
            sqlBinaryOpExprLeft.setLeft(sqlBinaryOpExprLeftLeft);
            sqlBinaryOpExprLeft.setOperator(SQLBinaryOperator.BooleanAnd);
            sqlBinaryOpExprLeft.setRight(sqlBinaryOpExprLeftRight);

            //构建Join语句中Condition的结构
            sqlBinaryOpExprRoot.setRight(sqlBinaryOpExprRight);
            sqlBinaryOpExprRoot.setOperator(SQLBinaryOperator.BooleanAnd);
            sqlBinaryOpExprRoot.setLeft(sqlBinaryOpExprLeft);

            //构建Join语句的结构
            fromTableSource.setLeft(leftTableSource);
            fromTableSource.setRight(rightTableSource);
            fromTableSource.setJoinType(joinType);
            fromTableSource.setCondition(sqlBinaryOpExprRoot);


            //构造过滤数据权限的语句
            odpsSelectQueryBlock.addSelectItem(sqlSelectItem);
            odpsSelectQueryBlock.setFrom(fromTableSource);

            //更改from权限结构
            selectQueryBlock.setFrom(odpsSelectQueryBlock,sqlTbName);



        }else {
            System.out.println("SQLTableSource Other Condition");
        }
    }

    public SQLPropertyExpr getSQLPropertyExpr(String name , String owner){
        SQLPropertyExpr result = new SQLPropertyExpr();
        result.setName(name);
        result.setOwner(owner);
        return result;
    }












    public static final String sql=" INSERT OVERWRITE  TABLE   pri_result.dws_itm_platform_plot_trade_d \n" +
            " SELECT \n" +
            "    * \n" +
            " FROM \n" +
            "    dws.dws_itm_platform_plot_trade_d   \n" +
            "\n" +
            " WHERE \n" +
            "    dt='${date_ymd}';\n";


    public static final String sql1=" INSERT OVERWRITE  TABLE   pri_result.dws_itm_platform_plot_trade_d\n" +
            " SELECT \n" +
            "    dws_itm_platform_plot_trade_d.the_dt,\n" +
            "    dws_itm_platform_plot_trade_d.shop_id,\n" +
            "    dws_itm_platform_plot_trade_d.seller_id\n" +
            " FROM \n" +
            "    dws.dws_itm_platform_plot_trade_d  \n" +
            " where \n" +
            "    dws_itm_platform_plot_trade_d.dt='${date_ymd}' \n" +
            " OR  \n" +
            "    dws_itm_platform_plot_trade_d.seller_id='20140030'\n" +
            " ORDER BY\n" +
            "    dws_itm_platform_plot_trade_d.shop_id;\n";

    public static final String sql2=" INSERT OVERWRITE  TABLE   pri_result.dws_itm_platform_plot_trade_d\n" +
            " SELECT \n" +
            "    abc.the_dt,\n" +
            "    abc.shop_id,\n" +
            "    abc.seller_id\n" +
            " FROM \n" +
            "    (select * from dws.dws_itm_platform_plot_trade_d join dws.a ) abc  \n" +
            " WHERE \n" +
            "    abc.dt='${date_ymd}';\n";

    public static final String sql3="INSERT overwrite TABLE avwaefrwagqbw partition (pt)\n" +
            "SELECT a.zxbw\n" +
            "    , a.xbax\n" +
            "    , a.brand\n" +
            "    , a.cpu\n" +
            "    , a.xx_x3\n" +
            "    , a.nwxus\n" +
            "\t, a.xbax xbax_first\n" +
            "\t, a.xbax xbax_first\n" +
            "\t, b.xbax xbax_last\n" +
            "\t, b.xbax xbax_last\n" +
            "\t, first_time\n" +
            "\t, last_time\n" +
            "\t, (rn_lead - rn_lag) + 1 count\n" +
            "\t, province(b.xbax, b.xbax) province\n" +
            "\t, province_code(b.xbax, b.xbax) province_code\n" +
            "\t, city(b.xbax, b.xbax) city\n" +
            "\t, city_code(b.xbax, b.xbax) city_code\n" +
            "\t, a.pt\n" +
            "FROM (\n" +
            "\tSELECT row_number() over(partition by pt, zxbw, xbax, brand, cpu, xx_x3, nwxus order by rn_lead) rn\n" +
            "\t\t, zxbw\n" +
            "\t\t, xbax\n" +
            "\t\t, brand\n" +
            "\t\t, cpu\n" +
            "\t\t, xx_x3\n" +
            "\t\t, nwxus\n" +
            "\t\t, xbax\n" +
            "\t\t, xbax\n" +
            "\t\t, server_time first_time\n" +
            "\t\t, pt\n" +
            "\t\t, rn_lead\n" +
            "\tFROM (\n" +
            "\t\tSELECT row_number() over(partition by pt, zxbw, xbax, brand, cpu, xx_x3, nwxus order by server_time, xbax, xbax) rn_lead\n" +
            "\t\t\t, zxbw\n" +
            "\t\t\t, xbax\n" +
            "\t\t\t, brand\n" +
            "\t\t\t, cpu\n" +
            "\t\t\t, xx_x3\n" +
            "\t\t\t, nwxus\n" +
            "\t\t\t, xbax\n" +
            "\t\t\t, LEAD(xbax, 1, 0) over(partition by pt, zxbw, xbax, brand, cpu, xx_x3, nwxus order by server_time, xbax, xbax) xbax_lead\n" +
            "\t\t\t, xbax\n" +
            "\t\t\t, LEAD(xbax, 1, 0) over(partition by pt, zxbw, xbax, brand, cpu, xx_x3, nwxus order by server_time, xbax, xbax) xbax_lead\n" +
            "\t\t\t, server_time\n" +
            "\t\t\t, pt\n" +
            "\t\t\tFROM bqxef\n" +
            "\t\t\twhere length(zxbw) >= 15\n" +
            "\t\t\t\tAND xbax IS NOT NULL \n" +
            "\t\t\t\tAND xbax IS NOT NULL\n" +
            "\t\t\t\tAND abs(xbax) <= 180\n" +
            "\t\t\t\tAND abs(xbax) <= 90\n" +
            "\t\t\t\tAND xbax <> 0\n" +
            "\t\t\t\tAND xbax <> 0\n" +
            "\t\t\t\tAND pt >= ${bizdate}\n" +
            "\t\t) x1\n" +
            "\tWHERE round(xbax, 2) <> round(xbax_lead, 2)\n" +
            "\t\tOR round(xbax, 2) <> round(xbax_lead, 2)\n" +
            ") a inner join (\n" +
            "\tSELECT row_number() over(partition by pt, zxbw, xbax, brand, cpu, xx_x3, nwxus order by rn_lag) rn\n" +
            "\t\t, zxbw\n" +
            "\t\t, xbax\n" +
            "\t\t, brand\n" +
            "\t\t, cpu\n" +
            "\t\t, xx_x3\n" +
            "\t\t, nwxus\n" +
            "\t\t, xbax\n" +
            "\t\t, xbax\n" +
            "\t\t, server_time last_time\n" +
            "\t\t, pt\n" +
            "\t\t, rn_lag\n" +
            "\tFROM (\n" +
            "\t\tSELECT row_number() over(partition by pt, zxbw, xbax, brand, cpu, xx_x3, nwxus order by server_time, xbax, xbax) rn_lag\n" +
            "\t\t\t, zxbw\n" +
            "\t\t\t, xbax\n" +
            "\t\t\t, brand\n" +
            "\t\t\t, cpu\n" +
            "\t\t\t, xx_x3\n" +
            "\t\t\t, nwxus\n" +
            "\t\t\t, xbax\n" +
            "\t\t\t, LAG(xbax, 1, 0) over(partition by pt, zxbw, xbax, brand, cpu, xx_x3, nwxus order by server_time, xbax, xbax) xbax_lag\n" +
            "\t\t\t, xbax\n" +
            "\t\t\t, LAG(xbax, 1, 0) over(partition by pt, zxbw, xbax, brand, cpu, xx_x3, nwxus order by server_time, xbax, xbax) xbax_lag\n" +
            "\t\t\t, server_time\n" +
            "\t\t\t, pt\n" +
            "\t\t\tFROM bqxef\n" +
            "\t\t\twhere length(zxbw) >= 15\n" +
            "\t\t\t\tAND xbax IS NOT NULL \n" +
            "\t\t\t\tAND xbax IS NOT NULL\n" +
            "\t\t\t\tAND abs(xbax) <= 180\n" +
            "\t\t\t\tAND abs(xbax) <= 90\n" +
            "\t\t\t\tAND xbax <> 0\n" +
            "\t\t\t\tAND xbax <> 0\n" +
            "\t\t\t\tAND pt >= ${bizdate}\n" +
            "\t\t) x2\n" +
            "\tWHERE round(xbax, 2) <> round(xbax_lag, 2)\n" +
            "\t\tOR round(xbax, 2) <> round(xbax_lag, 2)\n" +
            ") b on a.pt = b.pt \n" +
            "\tAND a.zxbw = b.zxbw \n" +
            "\tAND a.xbax = b.xbax \n" +
            "\tAND a.brand = b.brand \n" +
            "\tAND a.cpu = b.cpu \n" +
            "\tAND a.xx_x3 = b.xx_x3 \n" +
            "\tAND a.nwxus = b.nwxus \n" +
            "\tAND a.rn = b.rn\n" +
            ";";

    public static final String sql4 ="INSERT OVERWRITE  TABLE   pri_result.dws_itm_platform_plot_trade_d\n" +
            "SELECT\n" +
            "  dws_itm_platform_plot_trade_d.the_dt,\n" +
            "  dws_itm_platform_plot_trade_d.shop_id,\n" +
            "  dws_itm_platform_plot_trade_d.seller_id,\n" +
            "  dws_itm_platform_plot_trade_d.sku_id,\n" +
            "  dws_itm_platform_plot_trade_d.gmv_platform,\n" +
            "  dws_itm_platform_plot_trade_d.plot_type,\n" +
            "  dws_itm_platform_plot_trade_d.jdpay_trade_num,\n" +
            "  dws_itm_platform_plot_trade_d.jdpay_auction_num,\n" +
            "  dws_itm_platform_plot_trade_d.jdpay_trade_amt,\n" +
            "  dws_itm_platform_plot_trade_d.jdpay_winner_num,\n" +
            "  dws_itm_platform_plot_trade_d.gmv_auction_num,\n" +
            "  dws_itm_platform_plot_trade_d.gmv_trade_amt,\n" +
            "  dws_itm_platform_plot_trade_d.gmv_trade_num,\n" +
            "  dws_itm_platform_plot_trade_d.gmv_winner_num\n" +
            "FROM\n" +
            "  dws.dws_itm_platform_plot_trade_d\n" +
            "WHERE\n" +
            "  dws_itm_platform_plot_trade_d.dt='${date_ymd}'\n" +
            ";";

    public static final String sql5 = "SELECT \n" +
            "    dws_itm_platform_plot_trade_d.*\n" +
            "FROM \n" +
            "    sys.jddp_isv_seller \n" +
            "JOIN \n" +
            "    dws.dws_itm_platform_plot_trade_d \n" +
            "ON \n" +
            "    jddp_isv_seller.seller_id = dws_itm_platform_plot_trade_d.seller_id \n" +
            "    AND jddp_isv_seller.appkey = '8E6EBC94169EA04BC6957157ABA534B0' \n" +
            "    AND jddp_isv_seller.enable_flag = '1' ";


}