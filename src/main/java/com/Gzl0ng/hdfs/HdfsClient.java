package com.Gzl0ng.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystemAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;

/*
客户端代码常用套路
1.获取一个客户端对象
2.执行相关的操作目录
3.关闭资源
hdfs zookeeper
 */
public class HdfsClient {

    private FileSystem fs;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //连接的集群nn地址
        URI uri = new URI("hdfs://node1:9000");
        //配置文件
        Configuration configuration = new Configuration();

        configuration.set("dfs.replication","2");
        //用户
        String user = "root";
        //1.获取客户端对象
        fs = FileSystem.get(uri, configuration,user);
    }

    @After
    public void close() throws IOException {
        //3.关闭资源
        fs.close();
    }

    //创建目录
    @Test
    public void testmkdir() throws URISyntaxException, IOException, InterruptedException {

        //2.创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan"));

    }

    /**
     * 参数优先级
     * hdfs-default.xml => hdfs-site.xml =>在项目资源目录下的配置文件 => 代码里面的配置优先级最高
     * @throws IOException
     */
    //上传
    @Test
    public void testPut() throws IOException {
        //参数解读：参数1：表示删除原数据，参数2：是否允许覆盖，参数3：原数据路径，参数4：目的地路径
        fs.copyFromLocalFile(false,true,new Path("E:\\sunwukong.txt"),new Path("/xiyou/huaguoshan"));
    }

    //文件下载
    @Test
    public void testGet() throws IOException {
        //参数解读 参数1：原文件是否删除 参数2：源文件路径 参数3：目标地址路径win 参数4：本地校验
        fs.copyToLocalFile(true,new Path("/xiyou/huaguoshan/sunwukong.txt"),new Path("E://sunwukong.txt"),true);
    }

    //删除
    @Test
    public void testRm() throws IOException {
        //参数解读 参数1：要删除的路径 参数2：是否递归删除
//        fs.delete(new Path("/xiyou"),false);

        //删除空目录
//        fs.delete(new Path("/siyou"),true);

        //删除非空目录
        fs.delete(new Path("/jinguo"),true);
    }

    //文件的更名和移动
    @Test
    public void testMv() throws IOException {
        //参数解读 参数1：原文件路径 参数2：目标文件路径
        //对文件名称的修改
//        fs.rename(new Path("/wcinput/word.txt"),new Path("/wcinput/ss.txt"));

        //文件的移动和更名
//        fs.rename(new Path("/wcinput/ss.txt"),new Path("/cls.txt"));

        //目录更名
        fs.rename(new Path("/wcinput"),new Path("/output"));
    }

    //获取文件详情
    @Test
    public void fileDetail() throws IOException {
        //获取所有文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        //遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("============" + fileStatus.getPath() + "==========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    //判断是文件夹还是文件
    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus status:listStatus){
            if (status.isFile()){
                System.out.println("文件：" + status.getPath().getName());
            }else {
                System.out.println("目录：" + status.getPath().getName());
            }
        }
    }
}
