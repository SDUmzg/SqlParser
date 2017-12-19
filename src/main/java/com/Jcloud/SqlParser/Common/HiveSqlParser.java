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
import com.alibaba.druid.util.JdbcConstants;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by mzg on 2017/12/19.
 */
@Component
public class HiveSqlParser {

    public final String dbType = JdbcConstants.ODPS;

    public SqlResult HiveInsertParser(String sql,String appkey){
        SqlResult result = new SqlResult();
        try{
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
            SQLStatement stmt = stmtList.get(0);
            OdpsInsertStatement odstmt = (OdpsInsertStatement)stmt;
            OdpsInsert odpsInsert = odstmt.getItems().get(0);
            SQLSelect sqlSelect = odpsInsert.getQuery();
            OdpsSelectQueryBlock odpsSelectQueryBlock = (OdpsSelectQueryBlock) sqlSelect.getQueryBlock();
            getAuthOdpsSelectQueryBlock(odpsSelectQueryBlock,appkey);
            sqlSelect.setQuery(odpsSelectQueryBlock);
            odpsInsert.setQuery(sqlSelect);
//            System.out.println(SQLUtils.toSQLString(odpsInsert,dbType));
            result.setStatue(true);
            result.setValue(SQLUtils.toSQLString(odpsInsert,dbType));
        }catch (Exception e){
            result.setStatue(false);
            result.setValue(e.getMessage());
        }
        return result;
    }

    public void getAuthOdpsSelectQueryBlock(OdpsSelectQueryBlock odpsSelectQueryBlock, String appkey){
        SQLTableSource from = odpsSelectQueryBlock.getFrom();
        if (from instanceof SQLExprTableSource){
            SQLSubqueryTableSource authSubqueryTableSource = getAuthSQLSubqueryTableSource(odpsSelectQueryBlock , appkey);
            odpsSelectQueryBlock.setFrom(authSubqueryTableSource);
        }else if (from instanceof SQLSubqueryTableSource){
            SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) from;
            SQLSelect sqlSelect = sqlSubqueryTableSource.getSelect();
            OdpsSelectQueryBlock sqlSelectQueryBlock = (OdpsSelectQueryBlock) sqlSelect.getQueryBlock();
            getAuthOdpsSelectQueryBlock(sqlSelectQueryBlock,appkey);
            odpsSelectQueryBlock.setFrom(sqlSelectQueryBlock,sqlSubqueryTableSource.getAlias());
        }else if (from instanceof SQLJoinTableSource){
            SQLJoinTableSource joinTableSource = (SQLJoinTableSource) from;

            //Join -> Left
            SQLTableSource joinLeft = joinTableSource.getLeft();
            setJoinAuth(joinTableSource,joinLeft,"left",appkey);
            setJoinSub(joinTableSource,joinLeft,"left",appkey);


            //Join -> Right
            SQLTableSource joinRight = joinTableSource.getRight();
            setJoinAuth(joinTableSource,joinRight,"right",appkey);
            setJoinSub(joinTableSource,joinRight,"right",appkey);

            //设置
            odpsSelectQueryBlock.setFrom(joinTableSource);
        }

    }

    public void setJoinSub(SQLJoinTableSource joinTableSource,SQLTableSource sqlTableSource,String choice,String appkey){
        if (sqlTableSource instanceof SQLSubqueryTableSource){
            SQLSubqueryTableSource sqlSubqueryTableSource = (SQLSubqueryTableSource) sqlTableSource;
            SQLSelect sqlSelect = sqlSubqueryTableSource.getSelect();
            OdpsSelectQueryBlock sqlSelectQueryBlock =(OdpsSelectQueryBlock) sqlSelect.getQueryBlock();
            getAuthOdpsSelectQueryBlock(sqlSelectQueryBlock,appkey);
            sqlSelect.setQuery(sqlSelectQueryBlock);
            sqlSubqueryTableSource.setSelect(sqlSelect);
            if (choice.equals("left")){
                joinTableSource.setLeft(sqlSubqueryTableSource);
            }else if (choice.equals("right")){
                joinTableSource.setRight(sqlSubqueryTableSource);
            }
        }
    }

    public void setJoinAuth(SQLJoinTableSource joinTableSource,SQLTableSource sqlTableSource,String choice,String appkey){
        if (sqlTableSource instanceof SQLExprTableSource){
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSource;
            SQLExpr sqlExpr = sqlExprTableSource.getExpr();
            String sqlTbName = getName(sqlExpr);
            SQLSubqueryTableSource sqlSubqueryTableSource = getAuthSQLSubqueryTableSource(sqlExpr,sqlTbName,appkey);
            if (choice.equals("left")){
                joinTableSource.setLeft(sqlSubqueryTableSource);
            }else if (choice.equals("right")){
                joinTableSource.setRight(sqlSubqueryTableSource);
            }
        }
    }

    public SQLSubqueryTableSource getAuthSQLSubqueryTableSource(OdpsSelectQueryBlock odpsSelectQueryBlock1,String appkey){
        SQLTableSource from = odpsSelectQueryBlock1.getFrom();
        SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) from;
        SQLExpr sqlExpr =  sqlExprTableSource.getExpr();
        String sqlTbName = getName(sqlExpr);

        SQLSubqueryTableSource result = getAuthSQLSubqueryTableSource(sqlExpr,sqlTbName,appkey);

        return result;

    }

    public SQLSubqueryTableSource getAuthSQLSubqueryTableSource(SQLExpr sqlExpr,String sqlTbName,String appkey){
        SQLSubqueryTableSource result = new SQLSubqueryTableSource();
        SQLSelect resultSelect = new SQLSelect();

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

        resultSelect.setQuery(odpsSelectQueryBlock);
        result.setSelect(resultSelect);
        result.setAlias(sqlTbName);

        return result;

    }

    public String getName(SQLExpr sqlExpr){
        String sqlTbName = "";
        if (sqlExpr instanceof SQLPropertyExpr){
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlExpr;
            sqlTbName = sqlPropertyExpr.getName();
        }else if (sqlExpr instanceof SQLIdentifierExpr){
            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr)sqlExpr;
            sqlTbName = sqlIdentifierExpr.getName();
        }
        return sqlTbName;
    }

    public SQLPropertyExpr getSQLPropertyExpr(String name , String owner){
        SQLPropertyExpr result = new SQLPropertyExpr();
        result.setName(name);
        result.setOwner(owner);
        return result;
    }
}
