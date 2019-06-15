package com.example.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class DeleteFile {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        fs.delete(new Path("/user/hadoop-twq/test_dir"), true);
    }
}
