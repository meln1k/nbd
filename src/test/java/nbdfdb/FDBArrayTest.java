/*
 * Copyright 2018 Sam Pullara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package nbdfdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import org.HdrHistogram.Histogram;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;

import static org.junit.Assert.*;

public class FDBArrayTest {

  private static FDBArray fdbArray;

  @BeforeClass
  public static void setup() {
    FDB fdb = FDB.selectAPIVersion(510);
    Database db = fdb.open();
    FDBArray.create(db, "testArray", 512);
    fdbArray = FDBArray.open(db, "testArray");
  }

  @AfterClass
  public static void cleanup() {
    fdbArray.delete();
  }

  @After
  @Before
  public void delete() {
    fdbArray.clear();
  }

  @Test
  public void testNotAlignedReadWrite() throws ExecutionException, InterruptedException {
    byte[] bytes = new byte[12345];
    Arrays.fill(bytes, (byte) 250);
    fdbArray.write(bytes, 10000).get();
    byte[] read = new byte[12345];
    fdbArray.read(read, 10000).get();
    assertArrayEquals(bytes, read);
    assertEquals((12345 / 512 + 1) * 512, fdbArray.usage().get().longValue());
  }

  @Test
  public void testAlignedReadWrite() throws ExecutionException, InterruptedException {
    byte[] bytes = new byte[8192];
    Arrays.fill(bytes, (byte) 137);

    fdbArray.write(bytes, 512).get();
    byte[] read = new byte[8192];
    fdbArray.read(read, 512).get();
    assertArrayEquals(bytes, read);
    assertEquals((8192 / 512 + 1) * 512, fdbArray.usage().get().longValue());
  }




  @Test
  @Ignore
  public void testRandomReadWrite() throws ExecutionException, InterruptedException {
    Random r = new Random(1337);
    for (int i = 0; i < 1000; i++) {
      int length = r.nextInt(10000);
      byte[] bytes = new byte[length];
      r.nextBytes(bytes);
      int offset = r.nextInt(100000);
      fdbArray.write(bytes, offset).get();
      byte[] read = new byte[length];
      fdbArray.read(read, offset).get();
      assertArrayEquals("Iteration: " + i + ", " + length + ", " + offset, bytes, read);
    }
    assertEquals((110000 / 512 + 1) * 512, fdbArray.usage().get().longValue());
  }

  @Test
  @Ignore
  public void testRandomReadWriteBenchmark() throws ExecutionException, InterruptedException {
    List<FDBArray> arrays = new ArrayList<>();
    FDBArray fdbArray = FDBArrayTest.fdbArray;
    try {
      for (int j = 0; j < 3; j++) {
        Histogram readLatencies = new Histogram(10000000000l, 5);
        Histogram writeLatencies = new Histogram(10000000000l, 5);
        Random r = new Random(1337);
        Semaphore semaphore = new Semaphore(100);
        int TOTAL = 10000;
        for (int i = 0; i < TOTAL; i++) {
          {
            int length = r.nextInt(10000);
            byte[] bytes = new byte[length];
            r.nextBytes(bytes);
            int offset = r.nextInt(100000000);
            semaphore.acquireUninterruptibly();
            long startWrite = System.nanoTime();
            fdbArray.write(bytes, offset).thenRun(() -> {
              semaphore.release();
              long writeLatency = System.nanoTime() - startWrite;
              writeLatencies.recordValue(writeLatency);
            });
          };
          {
            int length = r.nextInt(10000);
            int offset = r.nextInt(100000000);
            byte[] read = new byte[length];
            semaphore.acquireUninterruptibly();
            long startRead = System.nanoTime();
            fdbArray.read(read, offset).thenRun(() -> {
              semaphore.release();
              long readLatency = System.nanoTime() - startRead;
              readLatencies.recordValue(readLatency);
            });
          };
        }
        semaphore.acquireUninterruptibly(100);
        percentiles("Writes", writeLatencies);
        percentiles("Reads", readLatencies);
      }
    } finally {
      arrays.forEach((array) -> {
        try {
          System.out.println("Usage: " + array.usage().get());
        } catch (ExecutionException | InterruptedException e) {
          e.printStackTrace();
        }
        array.delete();
      });
      System.out.println("Usage: " + FDBArrayTest.fdbArray.usage().get());
    }
  }

  private void percentiles(final String title, Histogram h) {
    System.out.println(title + ": " +
            " Mean: " + h.getMean()/1e6 +
            " p50: " + h.getValueAtPercentile(50)/1e6 +
            " p95: " + h.getValueAtPercentile(95)/1e6 +
            " p99: " + h.getValueAtPercentile(99)/1e6 +
            " p999: " + h.getValueAtPercentile(999)/1e6 +
            " max: " + h.getMaxValue()/1e6
    );
  }
}
