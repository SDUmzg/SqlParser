package com.Jcloud.SqlParser.Common;

import com.alibaba.druid.wall.WallCheckResult;
import com.alibaba.druid.wall.WallProvider;
import com.alibaba.druid.wall.spi.MySqlWallProvider;
import com.alibaba.druid.wall.spi.SQLServerWallProvider;

public class CheckInvaild 
{ 
 	public static Boolean evaluate(String sql, String dbType)
 	{ 
 		if (sql == null || dbType == null) 
 		{
 			return false;
 		}

 		//实例化一个WallProvider对象
 		WallProvider provider = null;
	 	if ("mssql".equalsIgnoreCase(dbType)) 
	 	{
	 		provider = new SQLServerWallProvider();
	 	} 
	 	else if ("mysql".equalsIgnoreCase(dbType)) 
	 	{
	 		/*
	 		 	实例化出一个MySqlWallProvider，这个类继承了WallProvider父类 
	 		 */
	 		provider = new MySqlWallProvider(); 
	 	} 
	 	else 
	 	{
	 		return false;
	 	}
	 	 
	 	/*
	 	  provider.getConfig()返回的就是之前实例化好的WallConfig对象，这个对象和规则有关
	 	*/
	 	provider.getConfig().setStrictSyntaxCheck(false);
	 	provider.getConfig().setMultiStatementAllow(true);
	 	provider.getConfig().setMultiStatementAllow(true);
	 	provider.getConfig().setConditionAndAlwayFalseAllow(true);
	 	provider.getConfig().setNoneBaseStatementAllow(true);
	 	provider.getConfig().setLimitZeroAllow(true);
	 	provider.getConfig().setConditionDoubleConstAllow(true);
 		provider.getConfig().setCommentAllow(true);

 		/*关键的判断逻辑入口点
 		 WallProvider提供了对数据库SQL拦截行为的抽象，这种架构的可扩展性很好，之后如果出现了不同数据库只要从这个父类WallProvider进行继承就好了
 		 例如 MySqlWallProvider就是从 父类WallProvider继承而来的
 		*/
 		
 		/*
 		 WallCheckResult 这个类是负责进行对SQL语句进行解析、遍历
 		 */
 		//return !provider.checkValid(sql);
 		WallCheckResult result = provider.check(sql);
        if (result.getViolations().size() > 0) 
        {
        	System.out.println(result.getViolations().get(0).getErrorCode());
            System.out.println(result.getViolations().get(0).getMessage());
            System.out.println(result.getViolations().get(0).toString());
            return false;
        }
        return true;
 	}

 	public static void main(String[] args) 
 	{
 		CheckInvaild checkInvaild = new CheckInvaild();
// 		System.out.println(checkInvaild.evaluate("select * from users where id=1 having 1=(nullif(ascii((SUBSTRING(user,1,1))),0));", "mssql"));
//		System.out.println(checkInvaild.evaluate("select * from user where id = '123456' and pwd ='1 = 1'", "mysql"));
//		String sql = "select max(abc.aaa) from (select sa,avg(g) as aaa   from s join sc on s.id = sc.sid join c on c.id = sc.cid group by s.id) as abc  where abc.sa = 21";
//		System.out.println(checkInvaild.evaluate(sql, "mysql"));
//		boolean a=checkInvaild.evaluate("select * from user ", "mysql");
		boolean a=checkInvaild.evaluate("select count(*) from logintest where id = '1666188122@qq.com' and password = '' or 1 = '1'","mssql");
		System.out.println(a);
	}
} 