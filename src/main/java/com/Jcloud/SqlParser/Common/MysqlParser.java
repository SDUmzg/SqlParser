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

import java.util.ArrayList;
import java.util.List;
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
            System.out.println(whereRight.toString());
        }else {
            queryBlock.setWhere(getWhereRight(seller_id));
        }
        //设置Limit
        if (limit==null || getLimitNum(limit)>200){
            queryBlock.setLimit(getLimit(200));
        }
        String resultStr = SQLUtils.toSQLString(queryBlock,dbType);
        if (TranslateTo.equals("kylin")){
            resultStr =  MysqlToKylin(resultStr);
        }
        String sqlresult = setBracket(resultStr);
        System.out.println(sqlresult);
        result.setStatue(true);
        result.setValue(sqlresult);
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
        List<String> regResult = new ArrayList<>();
        while (matcher.find()){
            boolean uniqueStr = true;
            int size = regResult.size();
            String str =matcher.group();
            for (int i = 0; i<size;i++){
                String temp = regResult.get(i);
                if (str.equals(temp)){
                    uniqueStr = false;
                }
            }
            if (uniqueStr){
                regResult.add(matcher.group());
            }
        }
        int size = regResult.size();
        for (int i=0 ;i<size;i++){
            String choiceStr = regResult.get(i);
            String temp = "CAST( "+choiceStr+" as varchar)";
            sql = sql.replace(choiceStr,temp);
        }
        String result = SQLUtils.format(sql,JdbcConstants.MYSQL);
        return result;
    }

    public String setBracket(String sql){
        int start = sql.lastIndexOf("WHERE")+5;
        int end = sql.lastIndexOf("AND");
        String subStr = sql.substring(start,end).trim();
        char startChar = subStr.charAt(0);
        char endChar = subStr.charAt(subStr.length()-1);
        StringBuffer stringBuffer = new StringBuffer(sql);
        if (!(startChar=='('&&endChar==')')){
            stringBuffer.insert(end-1,')');
            stringBuffer.insert(start+1,'(');
        }
        return stringBuffer.toString();

    }



}
