package org.apache.orc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;

public class FileScan {

  static double readFile(FileSystem fs,
                         Path path) throws IOException {
    long startTime = System.nanoTime();
    Reader reader = OrcFile.createReader(fs, path);

    RecordReader rows = reader.rows(null);
    long rowCount = 0;
    Object row = null;
    while (rows.hasNext()) {
      row = rows.next(row);
      rowCount += 1;
    }
    rows.close();
    double elapsedTime = (System.nanoTime() - startTime) / 1000000.0d;
    System.out.println("file: " + path + ", rows: " + rowCount + 
                       ", milliseconds: " + elapsedTime);
    return elapsedTime;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf).getRawFileSystem();
    double total = 0;
    for (int i=0; i < args.length; ++i) {
      total += readFile(fs, new Path(args[i]));
    }
    System.out.println("Total milliseconds: " + total);
  }
}