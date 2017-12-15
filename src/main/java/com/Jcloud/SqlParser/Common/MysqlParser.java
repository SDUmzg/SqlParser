package com.Jcloud.SqlParser.Common;

import com.Jcloud.SqlParser.Model.SqlResult;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLLimit;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcConstants;
import org.springframework.stereotype.Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by mzg on 2017/12/14.
 */
@Component
public class MysqlParser {
    final String dbType = JdbcConstants.MYSQL;

    /**
     * Mysql语句添加限制，转换成Kylin格式的数据库等
     * @param sqlStr      Mysql语句
     * @param TranslateTo   转化后的数据库格式 KYLIN ，Other
     * @param seller_id   ISV特定的ID
     * @return 返回结果
     */
    public SqlResult MysqlSelectParser(String sqlStr,String TranslateTo,String seller_id){
        SqlResult result = new SqlResult();
        String sql = SQLUtils.formatMySql(sqlStr);
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql,dbType);
        SQLSelectStatement stmt = (SQLSelectStatement) parser.parseStatement();
        SQLSelectQueryBlock queryBlock = stmt.getSelect().getQueryBlock();
        SQLExpr where =  queryBlock.getWhere();
        SQLLimit limit = queryBlock.getLimit();
        if (where!=null){
            SQLBinaryOpExpr whereLeft = (SQLBinaryOpExpr) where;
            SQLBinaryOpExpr whereRight =  getWhereRight(seller_id);
            SQLBinaryOpExpr whereNew = new SQLBinaryOpExpr();
            whereNew.setLeft(whereLeft);
            whereNew.setRight(whereRight);
            whereNew.setOperator(SQLBinaryOperator.BooleanAnd);
            queryBlock.setWhere(whereNew);
        }else {
            queryBlock.setWhere(getWhereRight(seller_id));
        }
        //设置Limit
        if (limit==null || getLimitNum(limit)>200){
            queryBlock.setLimit(getLimit(200));
        }
        String resultStr = SQLUtils.toSQLString(queryBlock,dbType);
//        System.out.println(resultStr);
        if (TranslateTo.equals("KYLIN")){
            resultStr =  MysqlToKylin(resultStr);
        }
        result.setStatue(true);
        result.setValue(resultStr);
        return result;
    }

    public SQLBinaryOpExpr getWhereRight(String seller_id){
        SQLBinaryOpExpr whereRight = new SQLBinaryOpExpr();
        SQLIdentifierExpr identifierExprLeft = new SQLIdentifierExpr();
        identifierExprLeft.setName("seller_id");
        SQLCharExpr charExprRight = new SQLCharExpr();
        charExprRight.setText(seller_id);
        whereRight.setLeft(identifierExprLeft);
        whereRight.setRight(charExprRight);
        whereRight.setOperator(SQLBinaryOperator.Equality);
        return whereRight;
    }

    public SQLLimit getLimit(int num){
        SQLLimit sqlLimit = new SQLLimit();
        sqlLimit.setRowCount(num);
        return sqlLimit;
    }

    public int getLimitNum(SQLLimit limit){
        SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr)limit.getRowCount();
        Number number = sqlIntegerExpr.getNumber();
        int result = number.intValue();
        return result;
    }

    public String MysqlToKylin(String sql){
        Pattern pattern = Pattern.compile("[\"'].*?[\"']");
        Matcher matcher = pattern.matcher(sql);
        while (matcher.find()){
            String temp = "CAST( "+matcher.group()+" as varchar)";
            sql = sql.replace(matcher.group(),temp);
        }
        String result = SQLUtils.format(sql,JdbcConstants.KYLIN);
        return result;
    }



}
