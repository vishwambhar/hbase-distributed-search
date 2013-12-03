package pl.touk.hbase.scanner.scan;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import pl.touk.hbase.scanner.model.Tariff;

class TariffProcessingCallback implements ProcessingCallback<Tariff> {
    List<Tariff> tmpPlaceholder;
    final int LIMIT = 10000;
    final BufferedWriter bw;

    private Logger log = Logger.getLogger(TariffProcessingCallback.class);

    TariffProcessingCallback(String startKey, String endKey) throws IOException {
        tmpPlaceholder = new ArrayList<Tariff>();

        File file = new File(String.format("/tmp/distributed_scan_results/%s_%s",startKey, endKey));
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        bw = new BufferedWriter(fw);
    }

    @Override
    public void process(Tariff obj) throws Exception {
        tmpPlaceholder.add(obj);

        if (tmpPlaceholder.size() > LIMIT) {
            dumpToFile();
        }
    }

    private void dumpToFile() throws IOException {
        for (Tariff t : tmpPlaceholder) bw.write(t.toString());
        tmpPlaceholder.clear();
    }

    @Override
    public void finish() throws Exception {
        dumpToFile();
        bw.close();
    }
}
