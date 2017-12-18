package com.Jcloud.SqlParser.Common;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Created by mzg on 2017/12/14.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MysqlParserTest {
    @Test
    public void mysqlSelectParser() throws Exception {
        MysqlParser mysqlParser = new MysqlParser();
        mysqlParser.MysqlSelectParser(sql4,"1","111");
//        System.out.println(mysqlParser.MysqlToKylin(sql5));
    }
    final String sql ="select max(abc.aaa),id from (select sa,avg(g) as aaa   from s join sc on s.id = sc.sid join c on c.id = sc.cid group by s.id) as abc  where abc.sa = 21 and abc.sb='a'";
    final String sql1 = "select * from tab where ( dt='dt' and (ok='ok' and a='1') or 1=1 ) and seller_id = '{seller_id }' limit 100";
    final String sql2 = "select * from tab where id = 'qazwsx' or 1=1";
    final String sql3 = "select dt from tab where dt = cast( '20171201' as varchar)";
    final String sql4 = "select * from tab where seller_id = 'qazwsx' limit 500 ";
    final String sql5 = "select * from (select * from tab where a1=\"a1\" and b1='b1')  where  a2='a2' or b2='b2'";
    final String sql6 = "select * from a";


}