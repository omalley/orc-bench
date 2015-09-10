package org.apache.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class FileWriter {

  public static class BigRow {
     long key1;
     Text f1 = new Text();
     Text f2 = new Text();
     Text f3 = new Text();
     Text f4 = new Text();
     Text f5 = new Text();
     Text f6 = new Text();
     Text f7 = new Text();
     Text f8 = new Text();
     Text f9 = new Text();
     Text f10 = new Text();
     Text f11 = new Text();
     Text f12 = new Text();
     Text f13 = new Text();
     Text f14 = new Text();
     Text f15 = new Text();
     Text f16 = new Text();
     Text f17 = new Text();
     Text f18 = new Text();
     Text f19 = new Text();
     Text f20 = new Text();
     Text f21 = new Text();
     Text f22 = new Text();
     Text f23 = new Text();
     Text f24 = new Text();
     Text f25 = new Text();
     Text f26 = new Text();
     Text f27 = new Text();
     Text f28 = new Text();
     Text f29 = new Text();
     Text f30 = new Text();
     Text f31 = new Text();
     Text f32 = new Text();
     Text f33 = new Text();
     Text f34 = new Text();
     Text f35 = new Text();
     Text f36 = new Text();
     Text f37 = new Text();
     Text f38 = new Text();
     Text f39 = new Text();
     Text f40 = new Text();
     Text f41 = new Text();
     Text f42 = new Text();
     Text f43 = new Text();
     Text f44 = new Text();
     Text f45 = new Text();
     Text f46 = new Text();
     Text f47 = new Text();
     Text f48 = new Text();
     Text f49 = new Text();
     Text f50 = new Text();
     long f51;
     long f52;
     Text f53 = new Text();

    public void set(long key1, String pattern, long fileNum) {
      this.key1 = key1;
      this.f1.set(pattern + "1");
      this.f2.set(pattern + "2");
      this.f3.set(pattern + "3");
      this.f4.set(pattern + "4");
      this.f5.set(pattern + "5");
      this.f6.set(pattern + "6");
      this.f7.set(pattern + "7");
      this.f8.set(pattern + "8");
      this.f9.set(pattern + "9");
      this.f10.set(pattern + "10");
      this.f11.set(pattern + "11");
      this.f12.set(pattern + "12");
      this.f13.set(pattern + "13");
      this.f14.set(pattern + "14");
      this.f15.set(pattern + "15");
      this.f16.set(pattern + "16");
      this.f17.set(pattern + "17");
      this.f18.set(pattern + "18");
      this.f19.set(pattern + "19");
      this.f20.set(pattern + "20");
      this.f21.set(pattern + "21");
      this.f22.set(pattern + "22");
      this.f23.set(pattern + "23");
      this.f24.set(pattern + "24");
      this.f25.set(pattern + "25");
      this.f26.set(pattern + "26");
      this.f27.set(pattern + "27");
      this.f28.set(pattern + "28");
      this.f29.set(pattern + "29");
      this.f30.set(pattern + "30");
      this.f31.set(pattern + "31");
      this.f32.set(pattern + "32");
      this.f33.set(pattern + "33");
      this.f34.set(pattern + "34");
      this.f35.set(pattern + "35");
      this.f36.set(pattern + "36");
      this.f37.set(pattern + "37");
      this.f38.set(pattern + "38");
      this.f39.set(pattern + "39");
      this.f40.set(pattern + "40");
      this.f41.set(pattern + "41");
      this.f42.set(pattern + "42");
      this.f43.set(pattern + "43");
      this.f44.set(pattern + "44");
      this.f45.set(pattern + "45");
      this.f46.set(pattern + "46");
      this.f47.set(pattern + "47");
      this.f48.set(pattern + "48");
      this.f49.set(pattern + "49");
      this.f50.set(pattern + "50");
      this.f51 = fileNum;
      this.f52 = fileNum;
      this.f53.set(" ");
    }
  }

  public static long writeFile(FileSystem fs, Path path, long numberOfRows,
                               ObjectInspector oi, Configuration conf, long file
                               ) throws IOException {
    long start = System.currentTimeMillis();
    Writer writer = OrcFile.createWriter(fs, path, conf, oi, 67108864,
        CompressionKind.ZLIB, 262144,10000);
    BigRow row = new BigRow();
    for (int i = 0; i < numberOfRows; i++) {
      row.set(i, i + "_" + file + "_This is a medium sized field value_", file);
      writer.addRow(row);
    }
    writer.close();
    long elapsed = System.currentTimeMillis() - start;
    System.out.println("File: " + path + " rows: " + numberOfRows + " ms: " +
        elapsed);
    return elapsed;
  }

  public static void main(String[] args) throws Exception {
    ObjectInspector inspector =
        ObjectInspectorFactory.getReflectionObjectInspector(
        BigRow.class,
        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf).getRawFileSystem();
    long rows = Long.parseLong(args[0]);
    long files = Long.parseLong(args[1]);
    String dir = "/tmp/orc-perf";
    fs.delete(new Path(dir));
    fs.mkdirs(new Path(dir));
    long total = 0;
    for(long i=0; i < files; ++i) {
      total += writeFile(fs, new Path(dir + "/orc_test_" + i), rows,
          inspector, conf, i);
    }
    System.out.println("Total = " + total);
  }
}
