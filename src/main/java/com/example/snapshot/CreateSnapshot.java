package com.example.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;

import java.io.IOException;
import java.net.URI;

public class CreateSnapshot {


    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();

        //hdfs dfsadmin 命令都需要指定一个工作目录
        HdfsAdmin hdfsAdmin = new HdfsAdmin(URI.create("/user/hadoop-twq"), configuration);
        hdfsAdmin.allowSnapshot(new Path("/user/hadoop-twq"));//允许这个路径可以创建快照,需要superuser权限
        hdfsAdmin.disallowSnapshot(new Path("/user/hadoop-twq"));

        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.createSnapshot(new Path("/user/hadoop-twq"), "snapshot-20190615");
        fileSystem.deleteSnapshot(new Path("/user/hadoop-twq"), "snapshot-20190615");
        ((DistributedFileSystem)fileSystem).getSnapshotDiffReport(new Path("/user/hadoop-twq"), "snapshot-20190615", "snapshot-20190616");
    }
}
