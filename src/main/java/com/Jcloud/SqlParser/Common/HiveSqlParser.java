package com.Jcloud.SqlParser.Common;

import com.Jcloud.SqlParser.Model.SqlResult;
import com.Jcloud.SqlParser.Model.SqlToken;
import com.Jcloud.SqlParser.Service.SellerService;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.odps.ast.OdpsInsert;
import com.alibaba.druid.sql.dialect.odps.ast.OdpsInsertStatement;
import com.alibaba.druid.sql.dialect.odps.ast.OdpsSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import com.sun.org.apache.bcel.internal.generic.IF_ACMPEQ;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.xml.bind.ValidationEventLocator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mzg on 2017/12/19.
 */
@Component
public class HiveSqlParser {
    @Autowired
    private SellerService sellerService;
    private static HiveSqlParser hiveSqlParser;

    @PostConstruct
    public void init(){
        hiveSqlParser = this;
        hiveSqlParser.sellerService = this.sellerService;
    }

    public final String dbType = JdbcConstants.ODPS;

    public SqlResult HiveInsertParser(String sql,String appkey){
        SqlResult result = new SqlResult();
        sql = preFormat(sql);
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
            setHivePartitionn(odpsInsert);
//            System.out.println(SQLUtils.toSQLString(odpsInsert,dbType));
            result.setStatue(true);
            result.setValue(SQLUtils.toSQLString(odpsInsert,dbType));
            //检测sql注入的方法
//            if (!CheckInvaild.evaluate(SQLUtils.toSQLString(odpsInsert,dbType),dbType)){
//                result.setStatue(false);
//                result.setValue("sql injection");
//            }
        }catch (Exception e){
            result.setStatue(false);
            result.setValue(e.getMessage());
        }
        return result;
    }

    public void getAuthOdpsSelectQueryBlock(OdpsSelectQueryBlock odpsSelectQueryBlock, String appkey){
        SQLTableSource from = odpsSelectQueryBlock.getFrom();
        if (from instanceof SQLExprTableSource){
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) from;
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlExprTableSource.getExpr();
            if (isSellerTb(sqlPropertyExpr)){
                SQLSubqueryTableSource authSubqueryTableSource = getAuthSQLSubqueryTableSource(odpsSelectQueryBlock , appkey);
                odpsSelectQueryBlock.setFrom(authSubqueryTableSource);
            }
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

    // Join 语句下面还有 比如  select * from tab1  或者   tab1 join tab2 on ...  子语句
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

    //Join的左右子树中没有其他语句，from tab1 ，这种形式的限权管理
    public void setJoinAuth(SQLJoinTableSource joinTableSource,SQLTableSource sqlTableSource,String choice,String appkey){
        if (sqlTableSource instanceof SQLExprTableSource){
            SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) sqlTableSource;
            SQLExpr sqlExpr = sqlExprTableSource.getExpr();
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlExpr;
            if (isSellerTb(sqlPropertyExpr)){
                String sqlTbName = getName(sqlExpr);
                String alias = sqlExprTableSource.getAlias();
                if (alias==null){
                    alias = sqlTbName ;
                }
                SQLSubqueryTableSource sqlSubqueryTableSource = getAuthSQLSubqueryTableSource(sqlExpr,sqlTbName,alias,appkey);
                if (choice.equals("left")){
                    joinTableSource.setLeft(sqlSubqueryTableSource);
                }else if (choice.equals("right")){
                    joinTableSource.setRight(sqlSubqueryTableSource);
                }
            }
        }
    }

    public SQLSubqueryTableSource getAuthSQLSubqueryTableSource(OdpsSelectQueryBlock odpsSelectQueryBlock,String appkey){
        SQLTableSource from = odpsSelectQueryBlock.getFrom();
        SQLExprTableSource sqlExprTableSource = (SQLExprTableSource) from;
        SQLExpr sqlExpr =  sqlExprTableSource.getExpr();
        String sqlTbName = getName(sqlExpr);
        String alias = sqlExprTableSource.getAlias();
        if (alias==null){
            alias = sqlTbName;
        }

        SQLSubqueryTableSource result = getAuthSQLSubqueryTableSource(sqlExpr,sqlTbName,alias,appkey);

        return result;

    }

    public SQLSubqueryTableSource getAuthSQLSubqueryTableSource(SQLExpr sqlExpr,String sqlTbName,String alias,String appkey){
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
        result.setAlias(alias);

        return result;

    }

    //在 from dws.abc  类似的语句中  ，获取表名 abc
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

    //构造 tab.a  类似结构
    public SQLPropertyExpr getSQLPropertyExpr(String name , String owner){
        SQLPropertyExpr result = new SQLPropertyExpr();
        result.setName(name);
        result.setOwner(owner);
        return result;
    }

    //对Partition的处理
    public void setHivePartitionn(OdpsInsert odpsInsert){
        //先构建一个Partition
        SQLAssignItem partition = new SQLAssignItem();
        SQLIdentifierExpr target = new SQLIdentifierExpr();
        target.setName("dt");
        SQLCharExpr value = new SQLCharExpr();
        value.setText("${date_ymd}");
        partition.setTarget(target);
        partition.setValue(value);

        List<SQLAssignItem> sqlAssignItemList = odpsInsert.getPartitions();
        int size = sqlAssignItemList.size();
        if (size==0){
            odpsInsert.addPartition(partition);
        }else {
            for(int i=0;i<size;i++){
                SQLAssignItem sqlAssignItem = sqlAssignItemList.get(i);
                String targetTemp = sqlAssignItem.getTarget().toString();
                String valueTemp = sqlAssignItem.getValue().toString();
                if (targetTemp.equals("dt")&&valueTemp.equals("'${date_ymd}'")){
                    return;
                }
            }
            sqlAssignItemList.add(partition);
            odpsInsert.setPartitions(sqlAssignItemList);
        }
    }

    //判断是否是卖家表
    public boolean isSellerTb(SQLPropertyExpr sqlPropertyExpr){
        boolean result = false;
        String targetTb = sqlPropertyExpr.toString();
        long count = hiveSqlParser.sellerService.countByTbName(targetTb);
        if (count>0){
            result = true;
        }
        return result;
    }

    //将From语句在前的Hive语句转换为可以解析的语句
    public String preFormat(String sql){
        String result = sql.trim();
        result = result.replace("from","FROM");
        result = result.replace("insert","INSERT");
        result = result.replace("where","WHERE");
        int fromNum = result.indexOf("FROM");
        int insertNum = result.indexOf("INSERT");
        if (insertNum>fromNum){
            String tempSql = result.substring(insertNum,result.length());
            String tempFrom = result.substring(0,insertNum);
            int whereNum = tempSql.indexOf("WHERE");
            if (whereNum==-1){
                whereNum = tempSql.length()-1;
            }
            StringBuffer stringBuffer = new StringBuffer(tempSql);
            stringBuffer.insert(whereNum,"  "+tempFrom);
            result = stringBuffer.toString();
            return result;
        }
        return result;
    }


    public String preFormatComplex(String sql){
        String result=sql.trim();
        result = result.replaceAll("\\bfrom\\b","FROM");
        result = result.replaceAll("\\binsert\\b","INSERT");
        result = result.replaceAll("\\bselect\\b","SELECT");
        result = result.replaceAll("\\bwhere\\b","WHERE");
        result = result.replaceAll("\\bjoin\\b","JOIN");
        result = result.replaceAll("\\bon\\b","ON");
        int fromNum = result.indexOf("FROM");
        int insertNum = result.indexOf("INSERT");
        int selectNum = result.lastIndexOf("SELECT");
        int whereNum = result.lastIndexOf("WHERE");
        int joinNum = result.lastIndexOf("JOIN");
        int onNum = result.lastIndexOf("ON");
        boolean haveJoin = true;
        if (joinNum!=-1&&result.substring(fromNum,joinNum).indexOf("FROM")!=-1){
            haveJoin = false ;
            joinNum = -1;
            onNum = -1;
        }
        List<Map<String,Object>> tokenList = new ArrayList<>();
        tokenList.add(getSqlTokenList(SqlToken.FROM,fromNum));
        tokenList.add(getSqlTokenList(SqlToken.INSERT,insertNum));
        tokenList.add(getSqlTokenList(SqlToken.SELECT,selectNum));
        tokenList.add(getSqlTokenList(SqlToken.WHERE,whereNum));
        tokenList.add(getSqlTokenList(SqlToken.JOIN,joinNum));
        tokenList.add(getSqlTokenList(SqlToken.ON,onNum));

        sortTokenList(tokenList);
        String insrtStr = "";
        String selectStr="";
        String fromStr = "";
        String whereStr = "";
        String joinStr = "";
        String onStr = "";


        int tokenListSize = tokenList.size();
        for (int i =0 ;i<tokenListSize;i++){
            Map<String,Object> tokenMapStart = tokenList.get(i);
            SqlToken tokenStart = (SqlToken) tokenMapStart.get("token");
            int valueStart = (int) tokenMapStart.get("value");
            if (valueStart==-1){
                continue;
            }
            int valueEnd = result.length();
            if (i!=tokenListSize-1){
                Map<String,Object> tokenMapEnd = tokenList.get(i+1);
                valueEnd = (int) tokenMapEnd.get("value");

            }
            switch (tokenStart){
                case INSERT:
                    insrtStr = result.substring(valueStart,valueEnd);
                    break;
                case SELECT:
                    selectStr = result.substring(valueStart,valueEnd);
                    break;
                case FROM:
                    fromStr = result.substring(valueStart,valueEnd);
                    String fromStrExFrom = fromStr.trim().substring(4);
                    if (fromStrExFrom.trim().substring(0,1).equals("(")){
                        int innerStart = fromStrExFrom.indexOf("(")+1;
                        int innerEnd = fromStrExFrom.lastIndexOf(")");
                        String fromSql = fromStrExFrom.substring(innerStart,innerEnd);
                        fromStr ="FROM  "+fromStrExFrom.substring(0,innerStart)+" "+preFormatComplex(fromSql)+" "+fromStrExFrom.substring(innerEnd);
                    }
                    break;
                case WHERE:
                    whereStr = result.substring(valueStart,valueEnd);
                    break;
                case JOIN:
                    if (haveJoin){
                        joinStr  = result.substring(valueStart,valueEnd);
                    }
                    break;
                case ON:
                    if (haveJoin){
                        onStr = result.substring(valueStart,valueEnd);
                    }
                    break;
                default:
                    break;
            }
        }
        String resultNew = insrtStr + " " + selectStr + " " + fromStr + " "+joinStr+" "+onStr+" "+whereStr;
        return resultNew;
    }

    public void sortTokenList(List<Map<String,Object>> tokenList){
        int tokenListSize = tokenList.size();

        for (int i =0 ; i<tokenListSize-1 ;i++){
            int k = i;
            Map<String,Object> tokenMap = tokenList.get(i);
            int value = (int) tokenMap.get("value");
            for (int j = k+1;j<tokenListSize;j++){
                Map<String,Object> tokenMapInner = tokenList.get(j);
                int valueInner = (int) tokenMapInner.get("value");
                if (value>valueInner){
                    k = j;
                    value = valueInner;
                }
            }
            if (k!=i){
                tokenList.set(i,tokenList.get(k));
                tokenList.set(k,tokenMap);
            }
        }

    }

    public Map<String,Object> getSqlTokenList(SqlToken sqlToken,int value){
        Map<String,Object> result = new HashMap<>();
        result.put("token",sqlToken);
        result.put("value",value);
        return result;
    }
}
