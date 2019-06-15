package com.example.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ListFile {

    private static Logger log = LoggerFactory.getLogger(ListFile.class);

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);

        //查看一个文件的属性信息
        FileStatus fileStatus = fs.getFileStatus(new Path("/user/hadoop-twq/test_dir/test_create.txt"));
        log.info("Owner权限:{},Group权限:{},Permission权限:{}", fileStatus.getOwner(),fileStatus.getGroup(),fileStatus.getPermission());

        log.info("----------------------------------------------------------------------");

        //查看一个目录下的文件的属性信息
        FileStatus[] fileStatuses = fs.listStatus(new Path("/user/hadoop-twq"));
        if(fileStatus == null || fileStatuses.length == 0){
            return;
        }

        for(FileStatus status: fileStatuses){
            log.info("文件名路径:{},Owner权限:{},Group权限:{},Permission权限:{}", status.getPath(), status.getOwner(),status.getGroup(),status.getPermission());

        }
    }
}
