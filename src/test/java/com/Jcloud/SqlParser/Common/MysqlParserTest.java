package com.Jcloud.SqlParser.Common;

import com.alibaba.druid.sql.visitor.functions.Char;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.swing.plaf.synth.SynthEditorPaneUI;

/**
 * Created by mzg on 2017/12/14.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MysqlParserTest {
    @Test
    public void mysqlSelectParser() throws Exception {
        MysqlParser mysqlParser = new MysqlParser();
        mysqlParser.MysqlSelectParser(sql7,"m","111");
//        System.out.println(mysqlParser.MysqlToKylin(sql7));
    }

    @Test
    public void test(){
        int start = sql8.lastIndexOf("WHERE");
        int end = sql8.lastIndexOf("AND");
        System.err.println("start  :  "+start);
        System.err.println("end   :   "+end);
        System.err.println(sql8.substring(start+6,end));
        String isvsql = sql8.substring(start+6,end).trim();
        char isv1 = isvsql.charAt(0);
        char isv2 = isvsql.charAt(isvsql.length()-1);
        System.err.println(isv1);
        System.err.println(isv2);

    }

    @Test
    public void test1(){
        String sq= sql8;
        System.err.println(setBracket(sq));
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



    final String sql ="select max(abc.aaa),id from (select sa,avg(g) as aaa   from s join sc on s.id = sc.sid join c on c.id = sc.cid group by s.id) as abc  where abc.sa = 21 and abc.sb='a'";
    final String sql1 = "select * from tab where ( dt='dt' and (ok='ok' and a='1') or 1=1 ) and seller_id = '{seller_id }' limit 100";
    final String sql2 = "select * from tab where id = 'qazwsx' or 1=1";
    final String sql3 = "select dt from tab where dt = cast( '20171201' as varchar)";
    final String sql4 = "select * from tab where seller_id = 'qazwsx' limit 500 ";
    final String sql5 = "select * from (select * from tab where a1=\"a1\" and b1='b1')  where  a2='a2' or b2='b2'";
    final String sql6 = "select * from a";
    final String sql7 = "select * from a where (id='1' and name = '1') and '1' in (1)";
    final String sql8 ="SELECT *\n" +
            "FROM (\n" +
            "\tSELECT *\n" +
            "\tFROM tab\n" +
            "\tWHERE a1 = \"a1\"\n" +
            "\t\tAND b1 = 'b1'\n" +
            ")\n" +
            "WHERE (a2 = 'a2'\n" +
            "\t\tOR b2 = 'b2')\n" +
            "\tAND seller_id = '111'\n" +
            "LIMIT 200";


}