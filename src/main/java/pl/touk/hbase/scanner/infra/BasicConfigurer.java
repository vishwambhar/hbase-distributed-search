package pl.touk.hbase.scanner.infra;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import pl.touk.hbase.scanner.scan.DistributedScan;

/**
 * @author mcl
 */
public class BasicConfigurer implements Configurer {

    private final CommandLine commandLine;

    public BasicConfigurer(CommandLine commandLine) {
        this.commandLine = commandLine;
    }

    @Override
    public boolean getCacheBlocks() {
        if (commandLine.hasOption(OPTION_CACHE_BLOCKS.shortOpt))
            return commandLine.hasOption(OPTION_CACHE_BLOCKS.shortOpt);
        else
            return true;
    }

    @Override
    public int getCaching() {
        if (commandLine.hasOption(OPTION_CACHING.shortOpt))
            return Integer.parseInt(commandLine.getOptionValue(OPTION_CACHING.shortOpt));
        else
            return DistributedScan.DONT_CHANGE;
    }

    @Override
    public int getBatch() {
        if (commandLine.hasOption(OPTION_BATCH.shortOpt))
            return Integer.parseInt(commandLine.getOptionValue(OPTION_BATCH.shortOpt));
        else
            return DistributedScan.DONT_CHANGE;
    }

    public static Options createOptions() {
        Options options = new Options();
        options.addOption(OPTION_BATCH.shortOpt, OPTION_BATCH.longOpt, OPTION_BATCH.hasParam, "how many rows in a batch");
        options.addOption(OPTION_CACHING.shortOpt, OPTION_CACHING.longOpt, OPTION_CACHING.hasParam,
                "how many rows in one fetch (reduce round-trips to server)");
        options.addOption(OPTION_CACHE_BLOCKS.shortOpt, OPTION_CACHE_BLOCKS.longOpt, OPTION_CACHE_BLOCKS.hasParam,
                "is block caching enabled ");
        return options;
    }

    private static OptionWrapper OPTION_BATCH = new OptionWrapper("b", "batch");
    private static OptionWrapper OPTION_CACHING = new OptionWrapper("c", "caching");
    private static OptionWrapper OPTION_CACHE_BLOCKS = new OptionWrapper("C", "cacheBlocks", false);


    private static class OptionWrapper {
        String shortOpt;
        String longOpt;
        boolean hasParam;

        private OptionWrapper(String shortOpt, String longOpt, boolean hasParam) {
            this.shortOpt = shortOpt;
            this.longOpt = longOpt;
            this.hasParam = hasParam;
        }

        private OptionWrapper(String shortOpt, String longOpt) {
            this(shortOpt, longOpt, true);
        }
    }
}
