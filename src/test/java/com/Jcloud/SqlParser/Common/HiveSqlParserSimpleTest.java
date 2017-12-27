package com.Jcloud.SqlParser.Common;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.util.JdbcConstants;
import org.junit.Test;
import java.util.List;
import java.util.Map;


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
        preFormat2(sql2);
    }

    @Test
    public void testHqlToSql(){

    }

    @Test
    public void test1(){
        SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(sql2, JdbcConstants.HSQL);
        List<SQLStatement> sqlStatementList=sqlStatementParser.parseStatementList();
        System.out.println("0");
    }

    @Test
    public void test2(){
    }

    public String preFormat2(String sql){
        String result=sql.trim();
        result = result.replace("from","FROM");
        result = result.replace("insert","INSERT");
        result = result.replace("select","SELECT");
        result = result.replace("where","WHERE");
        result = result.replace("join","JOIN");
        result = result.replace("on","ON");
        int fromNum = result.indexOf("FROM");
        int insertNum = result.indexOf("INSERT");
        int selectNum = result.lastIndexOf("SELECT");
        int whereNum = result.lastIndexOf("WHERE");
        int joinNum = result.lastIndexOf("JOIN");
        int onNum = result.lastIndexOf("ON");
        return result;
    }

    public List<Map<String,Integer>> getHiveItemList(){
        return null;
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

    public static final String hql = "from Student where studentName like :stuName and birthDay between :dat1 and :dat2";

    public static final String hql1 = "FROM pri_upload.tdy001 INSERT into TABLE pri_result.tdy001 PARTITION(dt = '${date_ymd}') SELECT name,age WHERE dt = '${date_ymd}'";


}