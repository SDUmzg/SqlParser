#SqlParser
若要把该项目部署在线上的Tomcat的容器中，请先将POM文件中SpringBoot内置的Tomcat插件移除。
具体操作：
将下面依赖
```
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
        <!-- 移除嵌入式tomcat插件 -->
        <exclusions>
            <exclusion>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-starter-tomcat</artifactId>
            </exclusion>
        </exclusions>
    </dependency>
```
更改为
```
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
        <!--&lt;!&ndash; 移除嵌入式tomcat插件 &ndash;&gt;-->
        <!--<exclusions>-->
            <!--<exclusion>-->
                <!--<groupId>org.springframework.boot</groupId>-->
                <!--<artifactId>spring-boot-starter-tomcat</artifactId>-->
            <!--</exclusion>-->
        <!--</exclusions>-->
    </dependency> 
```
之后正常打包即可