package com.megvii.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HdfsTest {
    Configuration configuration = null;
    private final String HDFS_PATH = "hdfs://Hadoop101:8020";
    FileSystem fileSystem = null;
    Path path = null;
    private final String user = "root";

    @Before
    public void init() throws Exception {
        /*
           构造一个访问指定HDFS系统的客户端对象
           第一个参数：HDFS的uri
           第二个参数：客户端指定的配置参数
           第三个参数：客户端的身份，也就是用户名
         */

        configuration = new Configuration();
        path = new Path("/hdfs/api/test");
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,user);
        System.out.println("init success");
    }

    @Test
    public void testMkdir() throws IOException {
        System.out.println("开始创建文件夹");
        boolean res = fileSystem.mkdirs(path);
        System.out.println("文件夹创建结束");
    }

    @After
    public void tearDown(){
        configuration = null;
        fileSystem = null;
        System.out.println("tear down");
    }
}
