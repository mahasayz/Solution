package com.datastax.driver.stress;

/**
 * Created by malam on 1/5/16.
 */

import agoda.dragonfruit.helper.SQLHelper;
import agoda.search.models.protobuf.HotelProtos;
import agoda.search.models.protobuf.Suppliers;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import joptsimple.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A simple stress tool to demonstrate the use of the driver.
 * <p/>
 * Sample usage:
 * stress insert -n 100000
 * stress read -n 10000
 */
public class Stress {

    private static final Map<String, QueryGenerator.Builder> generators = new HashMap<String, QueryGenerator.Builder>();

    private static Logger logger = Logger.getLogger(Stress.class.getName());

    static {
        PropertyConfigurator.configure(System.getProperty("log4j.configuration", "./conf/log4j.properties"));

        QueryGenerator.Builder[] gs = new QueryGenerator.Builder[]{
                Generators.INSERTER,
                Generators.READER
        };

        for (QueryGenerator.Builder b : gs)
            register(b.name(), b);
    }

    private static OptionParser defaultParser() {
        OptionParser parser = new OptionParser() {{
            accepts("h", "Show this help message");
            accepts("n", "Number of requests to perform (default: unlimited)").withRequiredArg().ofType(Integer.class);
            accepts("t", "Level of concurrency to use").withRequiredArg().ofType(Integer.class).defaultsTo(50);
            accepts("async", "Make asynchronous requests instead of blocking ones");
            accepts("ip", "The hosts ip to connect to").withRequiredArg().ofType(String.class).defaultsTo("127.0.0.1");
            accepts("report-file", "The name of csv file to use for reporting results").withRequiredArg().ofType(String.class).defaultsTo("last.csv");
            accepts("print-delay", "The delay in seconds at which to report on the console").withRequiredArg().ofType(Integer.class).defaultsTo(5);
            accepts("compression", "Use compression (SNAPPY)");
            accepts("connections-per-host", "The number of connections per hosts (default: based on the number of threads)").withRequiredArg().ofType(Integer.class);
            accepts("d", "Search Date").withRequiredArg().ofType(String.class);
            accepts("s", "Date offset before search date").withRequiredArg().ofType(Integer.class);
            accepts("p", "Date offset after search date").withRequiredArg().ofType(Integer.class);
            accepts("o", "CSV output for YCS rates").withRequiredArg().ofType(String.class);
            accepts("a", "CSV output for DMC rates").withRequiredArg().ofType(String.class);
        }};
        String msg = "Where <generator> can be one of " + generators.keySet() + '\n'
                + "You can get more help on a particular generator with: stress <generator> -h";
        parser.formatHelpWith(Help.formatFor("<generator>", msg));
        return parser;
    }

    public static void register(String name, QueryGenerator.Builder generator) {
        if (generators.containsKey(name))
            throw new IllegalStateException("There is already a generator registered with the name " + name);

        generators.put(name, generator);
    }

    private static class Stresser {
        private final QueryGenerator.Builder genBuilder;
        private final OptionParser parser;
        private final OptionSet options;

        private Stresser(QueryGenerator.Builder genBuilder, OptionParser parser, OptionSet options) {
            this.genBuilder = genBuilder;
            this.parser = parser;
            this.options = options;
        }

        public static Stresser forCommandLineArguments(String[] args) {
            OptionParser parser = defaultParser();

            String generatorName = findPotentialGenerator(args);
            if (generatorName == null) {
                // Still parse the options to handle -h
                OptionSet options = parseOptions(parser, args);
                logger.warning("Missing generator, you need to provide a generator.");
                printHelp(parser);
                System.exit(1);
            }

            if (!generators.containsKey(generatorName)) {
                logger.warning(String.format("Unknown generator '%s'", generatorName));
                printHelp(parser);
                System.exit(1);
            }

            QueryGenerator.Builder genBuilder = generators.get(generatorName);
            parser = genBuilder.addOptions(parser);
            OptionSet options = parseOptions(parser, args);

            List<?> nonOpts = options.nonOptionArguments();
            if (nonOpts.size() > 1) {
                logger.warning("Too many generators provided. Got " + nonOpts + " but only one generator supported.");
                printHelp(parser);
                System.exit(1);
            }

            return new Stresser(genBuilder, parser, options);
        }

        private static String findPotentialGenerator(String[] args) {
            for (String arg : args)
                if (!arg.startsWith("-"))
                    return arg;

            return null;
        }

        private static OptionSet parseOptions(OptionParser parser, String[] args) {
            try {
                OptionSet options = parser.parse(args);
                if (options.has("h")) {
                    printHelp(parser);
                    System.exit(0);
                }
                return options;
            } catch (Exception e) {
                logger.warning("Error parsing options: " + e.getMessage());
                printHelp(parser);
                System.exit(1);
                throw new AssertionError();
            }
        }

        private static void printHelp(OptionParser parser) {
            try {
                parser.printHelpOn(System.out);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        public OptionSet getOptions() {
            return options;
        }

        public void prepare(Session session) {
            genBuilder.prepare(options, session);
        }

        public QueryGenerator newGenerator(int id, List<Integer> hotelIds, Long checkIn, Session session, int iterations, PreparedStatement statement) {
            return genBuilder.create(id, hotelIds, checkIn, iterations, options, session, statement);
        }
    }

    public static class Help implements HelpFormatter {

        private final HelpFormatter defaultFormatter;
        private final String generator;
        private final String header;

        private Help(HelpFormatter defaultFormatter, String generator, String header) {
            this.defaultFormatter = defaultFormatter;
            this.generator = generator;
            this.header = header;
        }

        public static Help formatFor(String generator, String header) {
            // It's a pain in the ass to get the real console width in JAVA so hardcode it. But it's the 21th
            // century, we're not stuck at 80 characters anymore.
            int width = 120;
            return new Help(new BuiltinHelpFormatter(width, 4), generator, header);
        }

        public String format(Map<String, ? extends OptionDescriptor> options) {
            StringBuilder sb = new StringBuilder();

            sb.append("Usage: stress ").append(generator).append(" [<option>]*").append("\n\n");
            sb.append(header).append("\n\n");
            sb.append(defaultFormatter.format(options));
            return sb.toString();
        }
    }

    public static void main(String[] args) throws Exception {

        SQLHelper.init();
        List<Integer> hotelIds = SQLHelper.getAllHotelIds(new String[]{});


        Stresser stresser = Stresser.forCommandLineArguments(args);
        OptionSet options = stresser.getOptions();

        DateTimeFormatter format = DateTimeFormat.forPattern("yyyyMMdd");

        DateTime searchDate = format.parseDateTime((String)options.valueOf("d"));
        DateTime searchDateJoda = new DateTime(searchDate.getYear(), searchDate.getMonthOfYear(), searchDate.getDayOfMonth(), 0, 0, 0, DateTimeZone.forOffsetHours(7));

        int startOffset = (Integer) options.valueOf("s");
        int stopOffset = (Integer) options.valueOf("p");

//        DateTime searchDate = new DateTime(2016, 01, 22, 0, 0, 0, DateTimeZone.forOffsetHours(7));
//        int requests = options.has("n") ? (Integer) options.valueOf("n") : -1;
        int requests = hotelIds.size();
        int concurrency = (Integer) options.valueOf("t");

        String reportFileName = (String) options.valueOf("report-file");

        boolean async = options.has("async");

        int iterations = (requests == -1 ? -1 : requests / concurrency);

        final int maxRequestsPerConnection = 128;
        int maxConnections = options.has("connections-per-host")
                ? (Integer) options.valueOf("connections-per-host")
                : concurrency / maxRequestsPerConnection + 1;

        PoolingOptions pools = new PoolingOptions();
        pools.setNewConnectionThreshold(HostDistance.LOCAL, concurrency);
        pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);

        logger.info("Initializing stress test:");
        logger.info("  request count:        " + (requests == -1 ? "unlimited" : requests));
        logger.info("  concurrency:          " + concurrency + " (" + iterations + " requests/thread)");
        logger.info("  mode:                 " + (async ? "asynchronous" : "blocking"));
        logger.info("  per-host connections: " + maxConnections);
        logger.info("  compression:          " + options.has("compression"));

        try {
            // Create session to hosts
            Cluster cluster = new Cluster.Builder()
//                    .addContactPoints(String.valueOf(options.valueOf("ip")))
                    .addContactPoints("10.120.2.188", "10.120.2.189", "10.120.2.190")
//                    .addContactPoints("SG-AGDFCAS-6C01", "SG-AGDFCAS-6C02", "SG-AGDFCAS-6C03", "SG-AGDFCAS-6C04", "SG-AGDFCAS-6C05", "SG-AGDFCAS-6C06", "SG-AGDFCAS-6C07", "SG-AGDFCAS-6C08")
                    .withPoolingOptions(pools)
                    .withSocketOptions(new SocketOptions().setTcpNoDelay(true))
                    .build();

            if (options.has("compression"))
                cluster.getConfiguration().getProtocolOptions().setCompression(ProtocolOptions.Compression.SNAPPY);

            Session session = cluster.connect();

            Metadata metadata = cluster.getMetadata();
            logger.info(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));

            stresser.prepare(session);
            final PreparedStatement stmt = session.prepare("SELECT * FROM ratesilo WHERE hotelid = ? AND checkin = ? AND losbucket = 0");

            org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration();
            BufferedWriter priceWriter = null, supplierPriceWriter = null;
            FileSystem hdfs = null;
            try {
                hdfs = FileSystem.get(config);
            } catch (IOException e) {
                e.printStackTrace();
            }
//            String pathPrefixCsv = "out/maha-test-hadoop/CassData-", pathSupplierCsv = "out/maha-test-hadoop-supp/SuppData-";
            String pathPrefixCsv = (String) options.valueOf("o"), pathSupplierCsv = (String) options.valueOf("a");

            logger.info("  Search Date:   " + searchDateJoda.getMillis());
            logger.info("  Offset before: " + startOffset);
            logger.info("  Offset after:  " + stopOffset);
            logger.info("  YCS:           " + pathPrefixCsv);
            logger.info("  DMC:           " + pathSupplierCsv);

            List<AsynchronousConsumer> consumers = new ArrayList<AsynchronousConsumer>(concurrency);
            DateTime checkIn = null;
            QueryGenerator generator = null;
            Map<Integer, HotelProtos.Hotel> hotels = new HashMap<Integer, HotelProtos.Hotel>();
            Map<Integer, List<Suppliers.SupplierHotel>> supplierHotels = new HashMap<Integer, List<Suppliers.SupplierHotel>>();
            HotelProtos.Hotel hotel = null;
            for (int dayOffset = startOffset; dayOffset <= stopOffset; dayOffset++) {
                checkIn = searchDateJoda.plusDays(dayOffset);
//                Reporter reporter = new Reporter((Integer) options.valueOf("print-delay"), reportFileName, args, requests);

                logger.info("Preparing test for date: " + checkIn.toString() + "...");
                int fromIndex, toIndex;
                for (int i = 0; i < concurrency; i++) {
                    fromIndex = i * iterations;
                    toIndex = (fromIndex + iterations) > hotelIds.size() ?
                            hotelIds.size() :
                            (fromIndex + iterations);
                    generator = stresser.newGenerator(i, hotelIds.subList(fromIndex, toIndex), checkIn.getMillis(), session, iterations, stmt);
                    /*consumers[i] = async ? new AsynchronousConsumer(session, generator, reporter) :
                            new BlockingConsumer(session, generator, reporter);*/
                    consumers.add(new AsynchronousConsumer(session, generator));
                }

                logger.info("Starting to stress test...");

//                reporter.start();

                for (AsynchronousConsumer consumer : consumers)
                    consumer.start();

                for (AsynchronousConsumer consumer : consumers)
                    consumer.join();

                int ycsHotels = 0, dmcHotels = 0;
                String priceFilePath = pathPrefixCsv + checkIn.getMillis() + ".csv";
                String supplierPriceFilePath = pathSupplierCsv + checkIn.getMillis() + ".csv";
                priceWriter = new BufferedWriter( new OutputStreamWriter( hdfs.create( new Path(priceFilePath), true ), "UTF-8" ) );
                supplierPriceWriter = new BufferedWriter( new OutputStreamWriter( hdfs.create( new Path(supplierPriceFilePath), true ), "UTF-8" ) );

                for (AsynchronousConsumer consumer : consumers) {
                    hotels.putAll(consumer.getHotels());
                    supplierHotels.putAll(consumer.getSupplierHotels());
                }

                consumers.clear();

                for(Integer hotelId: hotelIds) {
                    hotel = null;

                    if (hotels.containsKey(hotelId)) {
                        hotel = hotels.get(hotelId);
                    }
                    if (hotel == null) {
                        if (supplierHotels.containsKey(hotelId)) {
                            for (Suppliers.SupplierHotel sHotel : supplierHotels.get(hotelId)) {
                                for (Suppliers.SupplierPriceInfo price : sHotel.getSupplierPriceInfosList()) {
                                    if (supplierPriceWriter != null)
                                        supplierPriceWriter.write(String.format(
                                                "%d\t%d\t%d\t%d\t%d\t%d\t%s\t%f\n",
                                                hotelId, price.getDmcID(), price.getAgodaRoomTypeID(), checkIn.getMillis(), price.getAgodaRatePlanID(), price.getOccupancy(), price.getCurrencyCode(), price.getSellIn()));
                                }
                            }
                        }
                        continue;
                    }
                    List<HotelProtos.PriceInfo> priceInfos =  hotel.getPriceInfosList();
                    for(HotelProtos.PriceInfo priceInfo : priceInfos) {
                        for (HotelProtos.KeyValuePair_Int32_RateCategory rate : priceInfo.getRateCategoriesList()) {
                            HotelProtos.RateCategory rateCategory = rate.getValue();
                            for (HotelProtos.KeyValuePair_DateTime_PriceDate priceDate : rateCategory.getDatesList()) {
                                HotelProtos.PriceDate price = priceDate.getValue();
                                for (HotelProtos.Price p : price.getPricesList()) {
                                    if (priceWriter != null) {
                                        priceWriter.write(String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
                                                hotelId, hotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn.getMillis(), priceInfo.getRatePlanID(), p.getType().name(), p.getApplyTo(), p.getOccupancy(), 0, p.getValue()));
                                    }
                                }
                            }
                        }

                        if (hotel.getSupplierID() != 332) {
                            for (HotelProtos.RateMatrix rateMatrix : priceInfo.getMatrixesList()) {
                                if (priceWriter != null) {
                                    priceWriter.write(String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
                                            hotelId, hotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn.getMillis(), priceInfo.getRatePlanID(), "Room", "PRPN", rateMatrix.getAdults(), rateMatrix.getChildren(), rateMatrix.getTotalRate()));
                                }
                            }
                        } else {
                            List<HotelProtos.KeyValuePair_DateTime_PriceDate> priceDates = priceInfo.getDatesList();
                            for (HotelProtos.KeyValuePair_DateTime_PriceDate priceDate : priceDates) {
                                for(HotelProtos.Price price : priceDate.getValue().getPricesList()) {
                                    if (priceWriter != null) {
                                        priceWriter.write(String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
                                                hotelId, hotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn.getMillis(), priceInfo.getRatePlanID(), price.getType().name(), price.getApplyTo(), price.getOccupancy(), 0, price.getValue()));
                                    }
                                }
                            }
                        }

                    }
                }

                hotels.clear();
                supplierHotels.clear();

//                reporter.stop();
                logger.info("Finished test for date: " + checkIn.toString() + "...");
            }

            logger.info("Stress test successful.");
            System.exit(0);

        } catch (NoHostAvailableException e) {
            logger.warning("No alive hosts to use: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            logger.warning("Unexpected error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}