package pl.touk.hbase.scanner.scan;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import pl.touk.hadoop.hbase.AnnotationDrivenRowMapper;
import pl.touk.hbase.scanner.model.Tariff;

/**
 * @author mcl
 */
public class TableScanner {

    public static final int POOL_SIZE = 10;
    private final HTablePool tablePool;
    private final AnnotationDrivenRowMapper<Tariff> rowMapper;

    private Logger log = Logger.getLogger(TableScanner.class);

    public TableScanner() {
        this(HBaseConfiguration.create());
    }

    public TableScanner(Configuration configuration) {
        rowMapper = new AnnotationDrivenRowMapper<Tariff>(Tariff.class);
        tablePool = new HTablePool(configuration, POOL_SIZE);
    }

    List<Tariff> scan(Scan scanCriteria, String tableName) {
        List<Tariff> retVal = new ArrayList<Tariff>();
        try {
            ResultScanner scanner = tablePool.getTable(tableName).getScanner(scanCriteria);
            Result result = scanner.next();

            int scanIterator = 0;
            while (result != null) {
                scanIterator++;
                retVal.add(rowMapper.map(result));
                result = scanner.next();
            }
            log.info("Iterated rows: " + scanIterator);
            scanner.close();
        } catch (Exception ex) {
            log.error("Error performing SCAN: ", ex);
        }
        return retVal;
    }

    void scan(Scan scanCriteria, String tableName, ProcessingCallback<Tariff> callback) {
        log.info(scanCriteria);
        try {
            ResultScanner scanner = tablePool.getTable(tableName).getScanner(scanCriteria);
            Result result = scanner.next();
            while (result != null) {
                callback.process(rowMapper.map(result));
                result = scanner.next();
            }
            callback.finish();
            scanner.close();
        } catch (Exception ex) {
            log.error("Error performing SCAN: ", ex);
        }
    }
}
