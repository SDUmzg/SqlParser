package com.Jcloud.SqlParser.Dao;

import com.Jcloud.SqlParser.Model.SqlModel.MysqlTestModel;
import com.Jcloud.SqlParser.Model.SqlModel.MysqlTestModelExample;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
@Mapper
public interface MysqlTestModelMapper {
    long countByExample(MysqlTestModelExample example);

    int deleteByExample(MysqlTestModelExample example);

    int deleteByPrimaryKey(Integer id);

    int insert(MysqlTestModel record);

    int insertSelective(MysqlTestModel record);

    List<MysqlTestModel> selectByExample(MysqlTestModelExample example);

    MysqlTestModel selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") MysqlTestModel record, @Param("example") MysqlTestModelExample example);

    int updateByExample(@Param("record") MysqlTestModel record, @Param("example") MysqlTestModelExample example);

    int updateByPrimaryKeySelective(MysqlTestModel record);

    int updateByPrimaryKey(MysqlTestModel record);
}