package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 使用Java API操作HDFS文件系统
 * 关键点：
 * 1) 创建Configuration
 * 2) 获取FileSystem
 * 3) HDFS API 操作
 */
public class HDFSAPP {

    public static final String HDFS_PATH = "hdfs://swarm-worker1:9000";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    /**
     * 构造一个访问指定HDFS系统的客户端对象
     * 第一个参数：HDFS的URI
     * 第二个参数：客户端指定的配置参数
     * 第三个参数：客户端的身份，即用户名
     *
     * @throws URISyntaxException
     */
    @Before
    public void setUp() throws URISyntaxException, IOException, InterruptedException {
        System.out.println("--------setUp---------");
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        fileSystem = FileSystem.get(new URI("hdfs://swarm-worker1:9000"), configuration, "iie4bu");
    }

    /**
     * 创建HDFS文件夹
     *
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/hdfsapi/test/myDir");
        boolean mkdirs = fileSystem.mkdirs(path);
        System.out.println(mkdirs);
    }

    /**
     * 查看HDFS内容
     */
    @Test
    public void text() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    /**
     * 创建文件
     *
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/helloworld.txt"));
        out.write(("hello   world\n").getBytes());
        out.write(("hello   world   vincent\n").getBytes());
        out.flush();
        out.close();
    }

    @Test
    public void testReplication() {
        System.out.println(configuration.get("dfs.replication"));
    }

    /**
     * 重命名文件
     *
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        Path src = new Path("/hdfsapi/test/b.txt");
        Path dst = new Path("/hdfsapi/test/c.txt");
        boolean rename = fileSystem.rename(src, dst);
        System.out.println(rename);
    }

    /**
     * 小文件
     * 拷贝本地文件到HDFS文件系统
     * 将本地的E:/test/uid_person.txt文件拷贝到hdfs上的路径/hdfsapi/test/下
     *
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path src = new Path("C:\\Users\\vincent\\Desktop\\helloworld.txt");
        Path dst = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src, dst);
    }

    /**
     * 大文件
     * 拷贝本地文件到HDFS文件系统：带进度
     * 将本地的E:/test/uid_person.txt文件拷贝到hdfs上的路径/hdfsapi/test/my.txt
     *
     * @throws Exception
     */
    @Test
    public void copyFromLocalBigFile() throws Exception {

        InputStream in= new BufferedInputStream(new FileInputStream(new File("E:/tools/linux/jdk-8u101-linux-x64.tar.gz")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/jdk.tar.gz"), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096);
    }

    /**
     * 拷贝HDFS文件到本地：下载
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path src = new Path("/hdfsapi/test/c.txt");
        Path dst = new Path("E:/test/a.txt");
        fileSystem.copyToLocalFile(src, dst);
    }

    /**
     * 查看目标文件夹下的所有文件
     * @throws Exception
     */
    @Test
    public void listFile() throws Exception {
        Path path = new Path("/hdfsapi/test/");
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        for(FileStatus file: fileStatuses) {
            String isDir = file.isDirectory() ? "文件夹": "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long len = file.getLen();
            String stringPath = file.getPath().toString();
            System.out.println("isDir:" + isDir + ", " + "permission: " + permission + ", " + "replication: " + replication + " , len: " + len + ", stringPath" + stringPath);
        }
    }

    /**
     * 递归查看目标文件夹下的所有文件
     * @throws Exception
     */
    @Test
    public void listFileRecursive() throws Exception {
        Path path = new Path("/hdfsapi/test/");
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            String isDir = file.isDirectory() ? "文件夹": "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long len = file.getLen();
            String stringPath = file.getPath().toString();
            System.out.println("isDir:" + isDir + ", " + "permission: " + permission + ", " + "replication: " + replication + " , len: " + len + ", stringPath" + stringPath);
        }
    }


    /**
     * 查看文件块信息
     * @throws Exception
     */
    @Test
    public void getFileBlockLocations() throws Exception {
        Path path = new Path("/hdfsapi/test/jdk.tar.gz");
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for(BlockLocation block: blocks) {
            for (String name: block.getNames()) {
                System.out.println(name + ": " + block.getOffset() + ": " + block.getLength());
            }
        }
    }

    /**
     * 删除文件
     * @throws Exception
     */
    @Test
    public void delete() throws Exception {
        boolean delete = fileSystem.delete(new Path("/hdfsapi/test/a.txt"), true);
        System.out.println(delete);
    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("--------tearDown---------");
    }

}
