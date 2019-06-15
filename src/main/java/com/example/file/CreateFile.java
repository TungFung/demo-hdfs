package com.example.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CreateFile {

    private static Logger log = LoggerFactory.getLogger(CreateFile.class);

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        /*
         * 在hadoop-twq目录下创建目录或文件，
         * 首先要清楚hadoop-twq这个目录是hadoop-twq创建的,我们用客户端程序连接hdfs会取当前OS的用户，这里既是Eric,也在supergroup里面
         * 那Eric就是个同组的，那要保证组权限中拥有Execute权限才能创建目录或文件.
         * 自己创建的目录，要在owner权限中添加Execute才能在其下创建子目录.
         * group和others权限中的write设置不了,全设置all也只能得到:drwxr-xr-x
         */
        fileSystem.mkdirs(new Path("/user/hadoop-twq/test_dir"), new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));

        //默认创建出来的文件权限是: -rw-r--r--
        FSDataOutputStream outputStream = fileSystem.create(new Path("/user/hadoop-twq/test_dir/test_create.txt"), new Progressable() {
            @Override
            public void progress() {
                log.info("创建中");
            }
        });

        //写入文件内容
        outputStream.write("Hello World".getBytes("UTF-8"));
        outputStream.close();
    }
}
