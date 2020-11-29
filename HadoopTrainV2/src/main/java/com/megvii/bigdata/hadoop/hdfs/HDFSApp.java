package com.megvii.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class HDFSApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String user = "root";   // 如果没有指定用户的会用本机的用户，没有写的权限
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://Hadoop101:8020"),configuration,user);

        Path path = new Path("/hdfs/api/test");
        boolean result = fileSystem.mkdirs(path);
        System.out.println(result);

    }
}
