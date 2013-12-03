package pl.touk.hbase.scanner.scan;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import pl.touk.hbase.scanner.infra.Configurer;

/**
 * @author mcl
 */
public class DistributedScan {

    public static int DONT_CHANGE = 0;

    private final Configuration configuration;

    private Logger log = Logger.getLogger(DistributedScan.class);

    public DistributedScan(Configuration config) {
        this.configuration = config;
    }

    public void performScan(Configurer configurer, long maxStamp, String tableName, int partitionsNum) throws InterruptedException {

        List<Pair> partitions = generatePartitionKeys(partitionsNum);

        final ExecutorService executor = Executors.newFixedThreadPool(partitions.size());

        final List<Callable<List>> tasks = createScanningTasks(partitions, configurer, maxStamp, tableName);
        final List<Future<List>> resultFromParts = executor.invokeAll(tasks, 10000, TimeUnit.SECONDS);

        executor.shutdown();
        awaitThreadsTermination(executor);
    }

    private List<Pair> generatePartitionKeys(int partitionsNum) {
        List<Pair> partitions = Lists.newArrayList();
        partitions.add(new Pair<Integer, Integer>(null, 1));
        partitions.add(new Pair<Integer, Integer>(partitionsNum, null));
        for (int i = 1;  i < partitionsNum; i++) {
            partitions.add(new Pair<Integer, Integer>(i, i + 1));
        }
        return partitions;
    }

    private List<Callable<List>> createScanningTasks(final List<Pair> partitions, final Configurer configurer, final long maxStamp, final String tableName) {
        List<Callable<List>> callables = Lists.newArrayList();

        for (final Pair<Integer, Integer> pair : partitions) {
            callables.add(new Callable<List>() {
                @Override
                public List call() throws Exception {
                    TableScanner tableScanner = new TableScanner(configuration);
                    Scan scanCriteria = buildScanCriteria(pair.getFirst(), pair.getSecond(),
                            configurer.getCaching(), configurer.getBatch(),
                            configurer.getCacheBlocks());
                    try {
                        scanCriteria.setTimeRange(0, maxStamp);
                        /*
                        OR sth like this:

                        scanCriteria.setFilter(new QualifierFilter(
                            CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(
                            Bytes.toBytes("trff_id_2010-02-08"))));*/
                    } catch (IOException e) {
                        log.error("Why is there an error when setting timerange??", e);
                    }

                    tableScanner.scan(scanCriteria, tableName,
                        new TariffProcessingCallback(pair.getFirst().toString(), pair.getSecond().toString()));
                    return null;
                }
            });
        }
        return callables;
    }

    /**
     * Handy blog post on configuring scan parameters: https://labs.ericsson.com/blog/hbase-performance-tuners
     */
    private Scan buildScanCriteria(Integer start, Integer end, int caching, int batch, boolean cacheBlocks) {
        Scan scanCriteria = new Scan();

        if (start != null)
            scanCriteria.setStartRow(start.toString().getBytes());
        if (end != null)
            scanCriteria.setStopRow(end.toString().getBytes());

        if (caching != DONT_CHANGE)             // set to 1000  500-10,000
            scanCriteria.setCaching(caching);  // determine how many rows are sent from a region
                                               // server to a client at the same time

        if (batch != DONT_CHANGE)
            scanCriteria.setBatch(batch); // change the setBatch parameter since it will give
                                          // you better control over your network bandwidth.

        scanCriteria.setCacheBlocks(cacheBlocks);  // disable the block cache, thus avoiding a lot of
                                                   // calls to the garbage collector
        return scanCriteria;
    }

    private void awaitThreadsTermination(ExecutorService executor) {
        long ts = new Date().getTime();
        while (!executor.isTerminated()) {
            if (new Date().getTime() - ts > 5000) {
                log.debug("Waiting for thread termination");
                ts  = new Date().getTime();
            }
        }
        log.info("All threads have finished");
    }
}
