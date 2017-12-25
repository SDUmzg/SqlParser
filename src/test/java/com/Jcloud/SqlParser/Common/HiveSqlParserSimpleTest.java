package com.Jcloud.SqlParser.Common;

import com.Jcloud.SqlParser.SqlParserApplication;
import com.alibaba.druid.sql.SQLUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

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

    public String preFormat2(){
        String result="";

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

}