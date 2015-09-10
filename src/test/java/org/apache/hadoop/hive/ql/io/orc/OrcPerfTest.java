package org.apache.hadoop.hive.ql.io.orc;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: planka
 * Date: 12/11/14
 * Time: 3:33 PM
 * Runs some performance tests for MergeIO & ORC reader.
 */
@RunWith(Enclosed.class)
public class OrcPerfTest {

  private static final String PROP_NUMBER_OF_THREADS = "test.mio.number.threads";
  private static final String PROP_CHECK_OUTPUT = "test.mio.check.output";
  private static final String PROP_WAIT_BEFORE_RUN = "test.wait.time.before.measure";
  private static final String TEST_HOME = "/tmp/morc_perftest";
  private static final String PERF_TEST_PREFIX = TEST_HOME + Path.SEPARATOR + "perf_test_orc_unc_";
  private static final String PERF_TEST_PREFIX_LARGE = TEST_HOME + Path.SEPARATOR + "perf_test_orc_unc_lg_";
  private static FileSystem fileSystem;
  private static boolean checkOutput;
  private static short waitBeforeRun;

  public OrcPerfTest() {
    // PASS
  }

  @BeforeClass
  public static void setUp() throws Exception {

    JobConf jobConf = new JobConf();
    jobConf.set("hive.exec.orc.write.format", "0.11");

    fileSystem = FileSystem.getLocal(jobConf).getRawFileSystem();
    fileSystem.delete(new Path(TEST_HOME));
    fileSystem.mkdirs(new Path(TEST_HOME));

    int numberOfThreads = Integer.parseInt(System.getProperty(PROP_NUMBER_OF_THREADS, "4"));
    ExecutorService pool = Executors.newFixedThreadPool(numberOfThreads);

    List<Future<String>> futures = new ArrayList<Future<String>>(40);

    for (int i = 0; i < 20; i++) {
      futures.add(pool.submit(new CreateFile(i, 100000, PERF_TEST_PREFIX, jobConf, fileSystem)));
    }

    for (int i = 0; i < 20; i++) {
      futures.add(pool.submit(new CreateFile(i, 1000000, PERF_TEST_PREFIX_LARGE, jobConf, fileSystem)));
    }

    // Wait for completion of tasks
    pool.shutdown();
    pool.awaitTermination(30, TimeUnit.MINUTES);
    int i = 0;
    for (Future<String> future : futures) {
      Assert.assertTrue(future.isDone());
    }

    checkOutput = Boolean.parseBoolean(System.getProperty(PROP_CHECK_OUTPUT, "false"));
    if (checkOutput) {
      System.out.println("The results of the output will be verified!!!");
    }

    waitBeforeRun = Short.parseShort(System.getProperty(PROP_WAIT_BEFORE_RUN, "0"));
    if (waitBeforeRun > 0){
      System.out.println("Will wait before running measurement!!!");
    }
  }

  @RunWith(Parameterized.class)
  public static class ORCReadTest {

    @SuppressWarnings("UnusedDeclaration")
    private final String testName;
    private final int leaderIdx;
    private final int noOfFiles;
    private final String filePrefix;
    private final long expectedRows;
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    public ORCReadTest(String testName, int leaderIdx, int noOfFiles, String filePrefix, long expectedRows) {
      this.testName = testName;
      this.leaderIdx = leaderIdx;
      this.noOfFiles = noOfFiles;
      this.filePrefix = filePrefix;
      this.expectedRows = expectedRows;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(
          new Object[][]{
              //{"1 file small", 0, 1, PERF_TEST_PREFIX, 100000},
              {"1 file large", 0, 1, PERF_TEST_PREFIX_LARGE, 1000000},
          }
      );
    }

    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    @Test
    public void testReadORCFile() throws Exception {
      JobConf jobConf = new JobConf();
      Reader reader;
      Path path;
      RecordReader rows;
      OrcStruct row;
      int i;
      if (waitBeforeRun > 0){
        System.out.println("Waiting before measure!!!");
        Thread.sleep(waitBeforeRun);
        System.out.println("Started!!!");
      }
      long elapsedTime = System.nanoTime();
      for (int k = 0; k < noOfFiles; k++) {
        path = new Path(filePrefix + k);
        reader = OrcFile.createReader(fileSystem, path);

        rows = reader.rows(null);
        i = 0;
        while (rows.hasNext()) {
          row = (OrcStruct) rows.next(null);
          if (checkOutput) {
            Assert.assertEquals(new LongWritable(i), row.getFieldValue(0));
            for (int j = 1; j < 51; j++) {
              Assert.assertEquals(
                  new Text(i + "_" + k + "_This is a medium sized field value" + "_" + j),
                  row.getFieldValue(j));
            }
            Assert.assertEquals(new LongWritable(k), row.getFieldValue(51));
            Assert.assertEquals(new LongWritable(k), row.getFieldValue(52));
            Assert.assertEquals(new Text(" "), row.getFieldValue(53));
          }
          i++;
        }
        Assert.assertEquals(expectedRows, i);
        rows.close();
      }
      elapsedTime = System.nanoTime() - elapsedTime;
      System.out.println("1File " + checkOutput + " Elapsed time in Nano seconds is: " + elapsedTime);
    }
  }

  @RunWith(Parameterized.class)
  @Ignore
  public static class ReadTest {

    @SuppressWarnings("UnusedDeclaration")
    private final String testName;
    private final int leaderIdx;
    private final int noOfFiles;
    private final String filePrefix;
    private final long expectedRows;
    @Rule
    public TestRule benchmarkRun = new BenchmarkRule();

    public ReadTest(String testName, int leaderIdx, int noOfFiles, String filePrefix, long expectedRows) {
      this.testName = testName;
      this.leaderIdx = leaderIdx;
      this.noOfFiles = noOfFiles;
      this.filePrefix = filePrefix;
      this.expectedRows = expectedRows;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
      return Arrays.asList(
          new Object[][]{
              {"1 file small", 0, 1, PERF_TEST_PREFIX, 100000},
              {"10 files small", 5, 10, PERF_TEST_PREFIX, 100000},
              {"20 files small", 10, 20, PERF_TEST_PREFIX, 100000},
              {"1 file large", 0, 1, PERF_TEST_PREFIX_LARGE, 1000000},
              {"10 files large", 5, 10, PERF_TEST_PREFIX_LARGE, 1000000},
              {"20 files large", 10, 20, PERF_TEST_PREFIX_LARGE, 1000000}
          }
      );
    }

    @SuppressWarnings("ConstantConditions")
    @BenchmarkOptions(benchmarkRounds = 1, warmupRounds = 0)
    @Test
    public void testReadORCFile() throws Exception {
      JobConf jobConf = new JobConf();
      Reader reader;
      Path path;
      RecordReader rows;
      OrcStruct row;
      int i;
      long elapsedTime = System.nanoTime();
      for (int k = 0; k < noOfFiles; k++) {
        path = new Path(filePrefix + k);
        // Force read of all the fields
        List<Integer> fields = new ArrayList<Integer>(5);
        for (int j = 0; j < 53; j++) {
          fields.add(j);
        }

        ColumnProjectionUtils.setReadColumnIDs(jobConf, fields);
        reader = OrcFile.createReader(fileSystem, path);

        rows = reader.rows(null);
        i = 0;
        while (rows.hasNext()) {
          if (checkOutput) {
            row = (OrcStruct) rows.next(null);
            Assert.assertEquals(new LongWritable(i), row.getFieldValue(0));
            for (int j = 1; j < 51; j++) {
              Assert.assertEquals(
                  new Text(i + "_" + k + "_This is a medium sized field value" + "_" + j),
                  row.getFieldValue(j));
            }
            Assert.assertEquals(new LongWritable(k), row.getFieldValue(51));
            Assert.assertEquals(new LongWritable(k), row.getFieldValue(52));
            Assert.assertEquals(new Text(" "), row.getFieldValue(53));
          }
          i++;
        }
        Assert.assertEquals(expectedRows, i);
        rows.close();
      }
      elapsedTime = System.nanoTime() - elapsedTime;
      System.out.println("1File " + checkOutput + " Elapsed time in Nano seconds is: " + elapsedTime);
    }
  }

  @Ignore
  public static class BigRow {
    final LongWritable key1;
    final Text f1;
    final Text f2;
    final Text f3;
    final Text f4;
    final Text f5;
    final Text f6;
    final Text f7;
    final Text f8;
    final Text f9;
    final Text f10;
    final Text f11;
    final Text f12;
    final Text f13;
    final Text f14;
    final Text f15;
    final Text f16;
    final Text f17;
    final Text f18;
    final Text f19;
    final Text f20;
    final Text f21;
    final Text f22;
    final Text f23;
    final Text f24;
    final Text f25;
    final Text f26;
    final Text f27;
    final Text f28;
    final Text f29;
    final Text f30;
    final Text f31;
    final Text f32;
    final Text f33;
    final Text f34;
    final Text f35;
    final Text f36;
    final Text f37;
    final Text f38;
    final Text f39;
    final Text f40;
    final Text f41;
    final Text f42;
    final Text f43;
    final Text f44;
    final Text f45;
    final Text f46;
    final Text f47;
    final Text f48;
    final Text f49;
    final Text f50;
    final LongWritable f51;
    final LongWritable f52;
    final Text f53;

    public BigRow(Long key1, String pattern, int fileNum) {
      this.key1 = new LongWritable(key1);
      this.f1 = new Text(pattern + "1");
      this.f2 = new Text(pattern + "2");
      this.f3 = new Text(pattern + "3");
      this.f4 = new Text(pattern + "4");
      this.f5 = new Text(pattern + "5");
      this.f6 = new Text(pattern + "6");
      this.f7 = new Text(pattern + "7");
      this.f8 = new Text(pattern + "8");
      this.f9 = new Text(pattern + "9");
      this.f10 = new Text(pattern + "10");
      this.f11 = new Text(pattern + "11");
      this.f12 = new Text(pattern + "12");
      this.f13 = new Text(pattern + "13");
      this.f14 = new Text(pattern + "14");
      this.f15 = new Text(pattern + "15");
      this.f16 = new Text(pattern + "16");
      this.f17 = new Text(pattern + "17");
      this.f18 = new Text(pattern + "18");
      this.f19 = new Text(pattern + "19");
      this.f20 = new Text(pattern + "20");
      this.f21 = new Text(pattern + "21");
      this.f22 = new Text(pattern + "22");
      this.f23 = new Text(pattern + "23");
      this.f24 = new Text(pattern + "24");
      this.f25 = new Text(pattern + "25");
      this.f26 = new Text(pattern + "26");
      this.f27 = new Text(pattern + "27");
      this.f28 = new Text(pattern + "28");
      this.f29 = new Text(pattern + "29");
      this.f30 = new Text(pattern + "30");
      this.f31 = new Text(pattern + "31");
      this.f32 = new Text(pattern + "32");
      this.f33 = new Text(pattern + "33");
      this.f34 = new Text(pattern + "34");
      this.f35 = new Text(pattern + "35");
      this.f36 = new Text(pattern + "36");
      this.f37 = new Text(pattern + "37");
      this.f38 = new Text(pattern + "38");
      this.f39 = new Text(pattern + "39");
      this.f40 = new Text(pattern + "40");
      this.f41 = new Text(pattern + "41");
      this.f42 = new Text(pattern + "42");
      this.f43 = new Text(pattern + "43");
      this.f44 = new Text(pattern + "44");
      this.f45 = new Text(pattern + "45");
      this.f46 = new Text(pattern + "46");
      this.f47 = new Text(pattern + "47");
      this.f48 = new Text(pattern + "48");
      this.f49 = new Text(pattern + "49");
      this.f50 = new Text(pattern + "50");
      this.f51 = new LongWritable(fileNum);
      this.f52 = new LongWritable(fileNum);
      this.f53 = new Text(" ");
    }

  }

  private static class CreateFile implements Callable<String> {

    private final int fileIdx;
    private final long numberOfRows;
    private final String prefix;
    private final JobConf jobConf;
    private final FileSystem fileSystem;

    private CreateFile(int fileIdx, long numberOfRows, String prefix, JobConf jobConf, FileSystem fileSystem) {
      this.fileIdx = fileIdx;
      this.numberOfRows = numberOfRows;
      this.prefix = prefix;
      this.jobConf = jobConf;
      this.fileSystem = fileSystem;
    }

    public String call() throws Exception {

      Writer writer = null;
      try {
        ObjectInspector inspector;
        inspector = ObjectInspectorFactory.getReflectionObjectInspector(
            BigRow.class,
            ObjectInspectorFactory.ObjectInspectorOptions.JAVA
        );

        writer = OrcFile.createWriter(
            fileSystem,
            new org.apache.hadoop.fs.Path(prefix + fileIdx),
            jobConf,
            inspector,
            67108864,
            CompressionKind.ZLIB,
            262144,
            10000
        );

        for (int i = 0; i < numberOfRows; i++) {
          writer.addRow(
              new BigRow(
                  (long) i,
                  i + "_" + fileIdx + "_This is a medium sized field value_",
                  fileIdx
              )
          );
        }
        return null;
      } finally {
        if (writer != null) {
          writer.close();
        }
      }

    }
  }
}
