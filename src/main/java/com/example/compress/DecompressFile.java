package com.example.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.*;

public class DecompressFile {

    public static void main(String[] args) throws Exception {
        String fileName = "myCompressFile.txt.gz";
        Configuration configuration = new Configuration();
        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(configuration);
        CompressionCodec compressionCodec = compressionCodecFactory.getCodec(new Path(fileName));//会自动根据文件类型选用相应的Codec实现
        if (compressionCodec == null) {
            System.out.println("Can not find codec for file " + fileName);
            return;
        }

        InputStream inputStream = compressionCodec.createInputStream(new FileInputStream(new File(fileName)));
        OutputStream outputStream = new FileOutputStream(new File("myDecompressFile.txt"));
        IOUtils.copyBytes(inputStream, outputStream, 4096, true);
    }
}
