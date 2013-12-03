package pl.touk.hbase.scanner;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hbase.HBaseConfiguration;

import pl.touk.hbase.scanner.infra.BasicConfigurer;
import pl.touk.hbase.scanner.infra.Configurer;
import pl.touk.hbase.scanner.scan.DistributedScan;

/**
 * @author mcl
 */
public class Runner {
    public static void main(String args[]) throws Exception {

        Configurer configurer = getConfigurer(args);

        DistributedScan distributedScan = new DistributedScan(HBaseConfiguration.create());

        distributedScan.performScan(configurer, 1262156995000L, "tariff_changes", 10);
    }

    private static Configurer getConfigurer(String[] args) throws Exception {
        Options options = BasicConfigurer.createOptions();
        CommandLineParser parser = new BasicParser();
        CommandLine cline = parser.parse(options, args);
        return new BasicConfigurer(cline);
    }
}
