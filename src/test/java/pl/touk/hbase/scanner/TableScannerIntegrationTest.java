package pl.touk.hbase.scanner;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import pl.touk.hbase.scanner.infra.BasicConfigurer;
import pl.touk.hbase.scanner.scan.DistributedScan;

public class TableScannerIntegrationTest {

    public static final String TEST_TABLE = "tariff_changes";
    public static final String FAMILY_NAME = "cf";
    public static final String COLUMN_NAME = "trff_id";

    public static final long MAX_STAMP = 1262156995000L;
    public static final int PARTITIONS_NUM = 10;
    public static final int MAX_KEY = 150;
    public static final int MIN_KEY = 0;

    private Logger log = Logger.getLogger(TableScannerIntegrationTest.class);

    static HBaseClusterWrapper cluster;

    @BeforeClass
    public static void setup() throws Exception {
        cluster = new HBaseClusterWrapper();
    }

    @Test
    public void shouldTestSomething() throws Exception {
        int samples = 100;
        prepareTestData(samples);

/*        Scan s = new Scan();
        s.setStartRow(Bytes.toBytes("1"));
        s.setStopRow(Bytes.toBytes("2"));
        HTable table = new HTable(cluster.getConfig(), TEST_TABLE);
        ResultScanner rs = table.getScanner(s);
        Result result = rs.next();
        AnnotationDrivenRowMapper<Tariff> rowMapper = new AnnotationDrivenRowMapper<Tariff>(Tariff.class);
        while (result != null) {
            log.info(rowMapper.map(result));
            result = rs.next();
        }
        rs.close();*/

        DistributedScan distributedScan = new DistributedScan(cluster.getConfig());
        // would like to have only half of those samples returned by the query :)
        distributedScan.performScan(new BasicConfigurer(mock(CommandLine.class)), MAX_STAMP + samples/2 * 1000,
            TEST_TABLE, PARTITIONS_NUM);
    }

    private void prepareTestData(int samples) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(cluster.getConfig());
        if (admin.tableExists(TEST_TABLE)) {
            admin.disableTable(TEST_TABLE);
            admin.deleteTable(TEST_TABLE);
        }
        admin.createTable(getTableDescriptor(), Bytes.toBytes(MIN_KEY), Bytes.toBytes(MAX_KEY), PARTITIONS_NUM);
        new HTable(cluster.getConfig(), TEST_TABLE).put(generateTariffChanges(samples));
    }

    private List<Put> generateTariffChanges(int counter) {
        List<Put> result = Lists.newArrayList();
        Random random = new Random();
        for (int i = 0; i < counter; i++) {
            Put testData = new Put(Bytes.toBytes("" + i));
            testData.add(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes(COLUMN_NAME), timestamp(i), Bytes.toBytes(random.nextInt()));
            result.add(testData);
        }
        log.debug("Inserting values: " + Joiner.on("\n").join(result));
        return result;
    }
    
    private long timestamp(long offset) {
        return MAX_STAMP + offset * 1000;
    }
    
    private HTableDescriptor getTableDescriptor() {
        HTableDescriptor descriptor = new HTableDescriptor(TEST_TABLE);
        descriptor.addFamily(new HColumnDescriptor(FAMILY_NAME));
        return descriptor;
    }

    @AfterClass
    public static void teardown() throws Exception {
        cluster.shutdown();
    }

}
