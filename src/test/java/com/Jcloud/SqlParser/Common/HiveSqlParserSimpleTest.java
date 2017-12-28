package com.Jcloud.SqlParser.Common;

import com.Jcloud.SqlParser.Model.SqlToken;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcConstants;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by mzg on 2017/12/25
 */
//@RunWith(SpringJUnit4ClassRunner.class)
//@SpringBootTest(classes = SqlParserApplication.class)
public class HiveSqlParserSimpleTest {

    @Test
    public void testHiveSQL(){
        String sqlTemp = SQLUtils.formatOdps(sql1);
        System.err.println(sqlTemp);
    }

    @Test
    public void testPreFormat(){
        String a = preFormat2(hql3);
        String b = SQLUtils.formatOdps(a);
        System.out.println(b);
    }

    @Test
    public void test2(){
        String pattern = ".*?appkey.*?'(.*?)'.*?";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(sql1);
        if (m.find()){
            String c = m.group(1);
            System.out.println(c);

        }
    }

    public String preFormat2(String sql){
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
//                        System.err.println(fromSql);
                        fromStr ="FROM  "+fromStrExFrom.substring(0,innerStart)+" "+preFormat2(fromSql)+" "+fromStrExFrom.substring(innerEnd);
//                        System.err.println(fromStr);
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




    public static final String sql="from  (from sys.jddp_isv_seller join dws.dws_itm_attention_d on (jddp_isv_seller.seller_id = dws_itm_attention_d.seller_id and jddp_isv_seller.appkey = 'ed6879c35879e0e1c2ae9ee63ed9fc62' and jddp_isv_seller.enable_flag = '1') select dws_itm_attention_d.*) dws_itm_attention_d insert overwrite table pri_upload.test partition(dt='${date_ymd}')  select shop_id,seller_id where dt = '${date_ymd-1}'";
    public static final String sql1 = "insert overwrite table pri_result.mystock partition(dt = '${date_ymd}')\n" +
            "select the_dt,seller_id,shop_id,auction_stock \n" +
            "from\n" +
            "    (select dws_seller_shop_stock_d.*\n" +
            "         from\n" +
            "        sys.jddp_isv_seller\n" +
            "         join\n" +
            "        dws.dws_seller_shop_stock_d\n" +
            "       on (\n" +
            "        jddp_isv_seller.seller_id = dws_seller_shop_stock_d.seller_id\n" +
            "        and jddp_isv_seller.appkey = 'ed6879c35879e0e1c2ae9ee63ed9fc62'\n" +
            "        and jddp_isv_seller.enable_flag = '1')\n" +
            "    ) dws_seller_shop_stock_d\n" +
            "where dt = '${date_ymd-1}'";
    public static final String sql2 =
            "FROM\n" +
                    "    (FROM\n" +
                    "        sys.jddp_isv_seller\n" +
                    "    join\n" +
                    "        dws.dws_seller_shop_stock_d\n" +
                    "    on (\n" +
                    "        jddp_isv_seller.seller_id = dws_seller_shop_stock_d.seller_id\n" +
                    "        and jddp_isv_seller.appkey = 'ed6879c35879e0e1c2ae9ee63ed9fc62'\n" +
                    "        and jddp_isv_seller.enable_flag = '1')\n" +
                    "    select dws_seller_shop_stock_d.*) dws_seller_shop_stock_d\n" +
                    "insert overwrite table pri_result.mystock partition(dt = '${date_ymd}')\n" +
                    "select the_dt,seller_id,shop_id,auction_stock\n" +
                    "where dt = '${date_ymd-1}'";

    public static final String sql3 =
            "FROM dws_seller_shop_stock_d\n" +
                    "insert overwrite table pri_result.mystock partition(dt = '${date_ymd}')\n" +
                    "select the_dt,seller_id,shop_id,auction_stock\n" +
                    "where dt = '${date_ymd-1}'";

    public static final String sql4 = "    FROM\n" +
            "        sys.jddp_isv_seller\n" +
            "    join\n" +
            "        dws.dws_seller_shop_stock_d\n" +
            "    on (\n" +
            "        jddp_isv_seller.seller_id = dws_seller_shop_stock_d.seller_id\n" +
            "        and jddp_isv_seller.appkey = 'ed6879c35879e0e1c2ae9ee63ed9fc62'\n" +
            "        and jddp_isv_seller.enable_flag = '1')\n" +
            "    select dws_seller_shop_stock_d.*\n" ;

    public static final String hql = "from Student where studentName like :stuName and birthDay between :dat1 and :dat2";

    public static final String hql1 = "FROM pri_upload.tdy001 INSERT into TABLE pri_result.tdy001 PARTITION(dt = '${date_ymd}') SELECT name,age WHERE dt = '${date_ymd}'";


    public static final String hql2="from  (from sys.jddp_isv_seller join dws.dws_itm_asso_d on (jddp_isv_seller.seller_id = dws_itm_asso_d.seller_id and jddp_isv_seller.appkey = '6acbc14f3ce443ffcd86502ae9df6ac9' and jddp_isv_seller.enable_flag = '1') select dws_itm_asso_d.*) dws_itm_asso_d insert overwrite table pri_temp.mytmp partition(dt = '${date_ymd}') select sku_id,shop_id where dt = '${date_ymd}'";

    public static final String hql3 ="from  (from sys.jddp_isv_seller join dws.dws_itm_attention_d on (jddp_isv_seller.seller_id = dws_itm_attention_d.seller_id and jddp_isv_seller.appkey = 'ed6879c35879e0e1c2ae9ee63ed9fc62' and jddp_isv_seller.enable_flag = '1') select dws_itm_attention_d.*) dws_itm_attention_d insert overwrite table pri_upload.test partition(dt='${date_ymd}')  select shop_id,seller_id where dt = '${date_ymd-1}'";
}