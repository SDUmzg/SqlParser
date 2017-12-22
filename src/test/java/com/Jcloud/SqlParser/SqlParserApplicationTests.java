package com.Jcloud.SqlParser;

import com.Jcloud.SqlParser.Dao.HiveTestModelMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SqlParserApplicationTests {

	@Autowired
	private HiveTestModelMapper hiveTestModelMapper;

	@Test
	public void contextLoads() {

	}


}
