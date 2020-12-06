package com.megvii.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsTest {
    private static final String HDFS_PATH = "hdfs://192.168.171.128:8020";
    private static final String HDFS_USER = "root";
    private static FileSystem fileSystem;

    @Before
    public void prepare() {
        try {
            Configuration configuration = new Configuration();
            // 这里我启动的是单节点的 Hadoop,所以副本系数设置为 1,默认值为 3
            configuration.set("dfs.replication", "1");
            fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    // 创建目录
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfs-api/test0"));
    }

    // 创建指定目录的权限
    @Test
    public void mkDirWithPermission() throws Exception {
        fileSystem.mkdirs(new Path("/hdfs-api/test1/"),
                new FsPermission(FsAction.READ_WRITE, FsAction.READ, FsAction.READ));
    }

    // 创建文件并写入内容
    @Test
    public void create() throws IOException {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfs-api/test/a.txt"),
                                                    true,4096);
        out.write("hello hadoop".getBytes());
        out.write("hello spark".getBytes());
        out.write("hello flink".getBytes());

        out.flush();
        out.close();
    }

    // 判断文件是否存在
    @Test
    public void exist() throws Exception {
        boolean exists = fileSystem.exists(new Path("/hdfs-api/test/a.txt"));
        System.out.println("文件" + (exists ? "存在" : "不存在"));
    }

    // 查看文件内容
    @Test
    public void readToString() throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path("/hdfs-api/test/a.txt"));
        String context = inputStreamToString(inputStream, "utf-8");
        System.out.println(context);
    }

    /**
     * 将输入流转换为指定编码的字符
     * @param inputStream 输入流
     * @param encode 指定编码类型
     * @return 解析字符串
     */
    private String inputStreamToString(InputStream inputStream,String encode) {
        if (encode == null || encode.equals("")){
            encode = "utf-8";
        }
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, encode));
            StringBuilder stringBuilder = new StringBuilder();
            String str = "";
            // 去掉最后两位的"/n"符号
            while((str = bufferedReader.readLine()) != null){
                stringBuilder.append(str).append("/n");
            }
            return stringBuilder.toString().substring(0,stringBuilder.length()-2);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 文件重命名
    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("hdfs-api/test/a.txt");
        Path newPath = new Path("hdfs-api/test/b.txt");
        boolean res = fileSystem.rename(oldPath, newPath);
        System.out.println("=====================");
        System.out.println(res);
        System.out.println("修改" + (res ? "成功" : "失败"));
        System.out.println("=====================");
    }

    // 删除目录或文件
    @Test
    public void delete() throws IOException {
        boolean res = fileSystem.delete(new Path("/hdfs-api/test/a.txt"), true);
        System.out.println("=====================");
        System.out.println(res);
        System.out.println("=====================");
    }

    // 上传文件到hdfs
    @Test
    public void copyFromLocalFile() throws IOException {
        Path src = new Path("D:\\hadoop-3.0.0\\bin\\hadoop.dll");
        Path des = new Path("/hdfs-api/test");
        fileSystem.copyFromLocalFile(src,des);
    }

    // 上传大文件并显示上传进度
    @Test
    public void copyFromLocalBigFile() throws Exception {

        File file = new File("F:\\BigData\\elasticsearch-6.1.2.tar.gz");
        final float fileSize = file.length();
        InputStream in = new BufferedInputStream(new FileInputStream(file));

        FSDataOutputStream out = fileSystem.create(new Path("/hdfs-api/test/elasticsearch-6.1.2.tar.gz"),
                new Progressable() {
                    long fileCount = 0;

                    public void progress() {
                        fileCount++;
                        // progress 方法每上传大约 64KB 的数据后就会被调用一次
                        System.out.println("上传进度：" + (fileCount * 64 * 1024 / fileSize) * 100 + " %");
                    }
                });
        IOUtils.copyBytes(in, out, 4096);
    }

    // 从hdfs上下载文件
    @Test
    public void copyToLocal() throws IOException {
        Path src = new Path("/hdfs-api/test/elasticsearch-6.1.2.tar.gz");
        Path des = new Path("F:\\BigData");
        fileSystem.copyToLocalFile(false,src,des,true);
    }

    // 查看指定目录下所有文件的信息
    @Test
    public void listFiles() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfs-api"));
        for (FileStatus fileStatu: fileStatuses ) {
            System.out.println(fileStatu.toString());
        }
    }

    // 递归查看指定目录下的所有文件信息
    @Test
    public void listFileRecursive() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfs-api"), true);

        while(files.hasNext()){
            System.out.println(files.next());
        }
    }

    // 查看文件块信息
    @Test
    public void getFileBlockLocations() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfs-api/test/hadoop.dll"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : blocks){
            System.out.println(block);
        }
    }

    @After
    public void destroy() {
        fileSystem = null;
    }
}