package com.Jcloud.SqlParser.Dao;

import com.Jcloud.SqlParser.Model.SqlModel.HiveTestModel;
import com.Jcloud.SqlParser.Model.SqlModel.HiveTestModelExample;
import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface HiveTestModelMapper {
    long countByExample(HiveTestModelExample example);

    int deleteByExample(HiveTestModelExample example);

    int deleteByPrimaryKey(Integer id);

    int insert(HiveTestModel record);

    int insertSelective(HiveTestModel record);

    List<HiveTestModel> selectByExample(HiveTestModelExample example);

    HiveTestModel selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") HiveTestModel record, @Param("example") HiveTestModelExample example);

    int updateByExample(@Param("record") HiveTestModel record, @Param("example") HiveTestModelExample example);

    int updateByPrimaryKeySelective(HiveTestModel record);

    int updateByPrimaryKey(HiveTestModel record);
}