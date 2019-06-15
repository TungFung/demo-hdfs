package com.example.file;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ReadFile {

    private static Logger log = LoggerFactory.getLogger(ReadFile.class);

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream inputStream = fs.open(new Path("/user/hadoop-twq/test_dir/test_create.txt"));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while((line = bufferedReader.readLine()) != null){
            log.info(line);
        }
        inputStream.close();
    }
}
