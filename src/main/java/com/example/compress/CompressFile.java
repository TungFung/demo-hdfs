package com.example.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * 使用Hadoop的压缩机制压缩文件
 * myCompressFile.txt -- 838B
 * myCompressFile.txt.gz -- 48B
 */
public class CompressFile {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        CompressionCodec compressionCodec = ReflectionUtils.newInstance(GzipCodec.class, configuration);

        String fileName = "myCompressFile.txt";
        File fileOut = new File(fileName + compressionCodec.getDefaultExtension());
        fileOut.delete();//删掉之前的压缩文件以支持重复执行

        CompressionOutputStream compressionOutputStream = compressionCodec.createOutputStream(new FileOutputStream(fileOut));

        InputStream inputStream = new FileInputStream(fileName);
        IOUtils.copyBytes(inputStream, compressionOutputStream, 4096, false);//读取文件流->压缩输出流

        inputStream.close();
        compressionOutputStream.close();
    }
}
