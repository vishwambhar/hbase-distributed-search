package pl.touk.hbase.scanner.infra;

/**
 * @author mcl
 */
public interface Configurer {

    boolean getCacheBlocks();
    int getCaching();
    int getBatch();

}