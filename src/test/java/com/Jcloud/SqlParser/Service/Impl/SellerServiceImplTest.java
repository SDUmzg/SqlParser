package com.Jcloud.SqlParser.Service.Impl;

import com.Jcloud.SqlParser.Model.SqlModel.SellerModel;
import com.Jcloud.SqlParser.Service.SellerService;
import com.Jcloud.SqlParser.SqlParserApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import static org.junit.Assert.*;

/**
 * Created by mzg on 2017/12/21.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = SqlParserApplication.class)
@WebAppConfiguration
public class SellerServiceImplTest {

    @Autowired
    private SellerService sellerService;

    @Test
    public void insertSeller() throws Exception {
        SellerModel sellerModel = new SellerModel();
        sellerModel.setName("ok");
    }

}