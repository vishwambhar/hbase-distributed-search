package pl.touk.hbase.scanner.scan;

interface ProcessingCallback<T> {
    void process(T obj) throws Exception;
    void finish() throws Exception;
}
