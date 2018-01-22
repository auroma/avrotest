package com.avro;

import com.avro.file.DataFileStream;
import com.avro.generic.GenericDatumReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * Created by AMIYA on 1/21/2018.
 */
public class main {
    public static void main(String[] args) throws IOException {
        BufferedInputStream inStream = null;
        String inputF = "D:\\SparkWorkspace\\Resource\\avro\\default.avsc";
        org.apache.hadoop.fs.Path inPath = new org.apache.hadoop.fs.Path(inputF);
        try {
            Configuration conf = new Configuration();
//            conf.set("fs.defaultFS", "hdfs://sandbox.hortonworks.com:8020");
            URI uri = inPath.toUri();
            FileSystem fs = FileSystem.get(uri, conf);
            inStream = new BufferedInputStream(fs.open(inPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        DataFileStream reader = new DataFileStream(inStream, new GenericDatumReader());
        Schema schema = reader.getSchema();
        LogicalType logicalType= schema.getLogicalType();

//        System.out.println(logicalName);
        System.out.println(schema.toString());
    }
}
