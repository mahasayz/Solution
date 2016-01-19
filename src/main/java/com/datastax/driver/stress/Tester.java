package com.datastax.driver.stress;

import agoda.dragonfruit.helper.HotelHelper;
import agoda.dragonfruit.helper.SQLHelper;
import agoda.dragonfruit.helper.SupplierHelper;
import agoda.search.models.protobuf.HotelProtos;
import agoda.search.models.protobuf.Suppliers;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.*;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;

/**
 * Created by malam on 1/15/16.
 */

class DemoConsumer implements Consumer {

    private static final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final Session session;
    private final List<Integer> requests;
    private final Iterator<Integer> iterator;
    private final PreparedStatement stmt;
    private long checkIn;

    public DemoConsumer(Session session, List requests, PreparedStatement stmt, long checkIn) {
        this.session = session;
        this.requests = requests;
        this.iterator = this.requests.iterator();
        this.stmt = stmt;
        this.checkIn = checkIn;
    }

    public void start() {
        executorService.execute(new Runnable() {
            public void run() {
                request();
            }
        });
    }

    private void request() {

        if (!this.iterator.hasNext()) {
            shutdown();
            return;
        }

        handle(this.iterator.next());
    }

    public void join() {
        awaitUninterruptibly(shutdownLatch);
    }

    public void handle(int content) {
//        System.out.println("Content = " + content);

        BoundStatement bs = stmt.bind();
        bs.setInt("hotelid", content);
        bs.setDate("checkin", new Date(checkIn));
        ResultSet rs = null;
        rs = session.execute(bs);

        Row row = rs.one();
        if (row != null) {

            try {
                BufferedWriter bw = null;
                String path = null;

                // SUPPLIER part
                Suppliers.SupplierHotel dmcHotel = null;
                StringBuilder sb = new StringBuilder();
                for (int supplierId : new int[]{3038, 27900}) {
                    dmcHotel = SupplierHelper.parseCsRow(row, supplierId);
                    if (dmcHotel != null)
                        break;;
                }

                if (dmcHotel != null) {
                    for (Suppliers.SupplierPriceInfo price : dmcHotel.getSupplierPriceInfosList())
                        sb.append(String.format(
                                "%d\t%d\t%d\t%d\t%d\t%d\t%s\t%f\n",
                                dmcHotel.getAgodaHotelID(), price.getDmcID(), price.getAgodaRoomTypeID(), checkIn, price.getAgodaRatePlanID(), price.getOccupancy(), price.getCurrencyCode(), price.getSellIn()));
                }

                if (sb.length() > 0) {
                    FileSystem hdfs = FileSystem.get(new Configuration());
                    path = "out/maha/" + checkIn + "-" + content + "-dmc.txt";
                    bw = new BufferedWriter( new OutputStreamWriter( hdfs.create( new Path(path), true ), "UTF-8" ) );
                    bw.write(sb.toString());
                    bw.flush();
                    bw.close();
                    hdfs.close();
                }

                sb.setLength(0);

                // YCS Hotel part
                HotelProtos.Hotel ycsHotel = HotelHelper.parseCsRow(row);
                if (ycsHotel != null) {
                    List<HotelProtos.PriceInfo> priceInfos = ycsHotel.getPriceInfosList();
                    for (HotelProtos.PriceInfo priceInfo : priceInfos) {
                        for (HotelProtos.KeyValuePair_Int32_RateCategory rate : priceInfo.getRateCategoriesList()) {
                            HotelProtos.RateCategory rateCategory = rate.getValue();
                            for (HotelProtos.KeyValuePair_DateTime_PriceDate priceDate : rateCategory.getDatesList()) {
                                HotelProtos.PriceDate price = priceDate.getValue();
                                for (HotelProtos.Price p : price.getPricesList()) {
                                    sb.append(String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
                                            ycsHotel.getID(), ycsHotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn, priceInfo.getRatePlanID(), p.getType().name(), p.getApplyTo(), p.getOccupancy(), 0, p.getValue()));
                                }
                            }
                        }

                        if (ycsHotel.getSupplierID() != 332) {
                            for (HotelProtos.RateMatrix rateMatrix : priceInfo.getMatrixesList()) {
                                sb.append(String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
                                        ycsHotel.getID(), ycsHotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn, priceInfo.getRatePlanID(), "Room", "PRPN", rateMatrix.getAdults(), rateMatrix.getChildren(), rateMatrix.getTotalRate()));
                            }
                        } else {
                            List<HotelProtos.KeyValuePair_DateTime_PriceDate> priceDates = priceInfo.getDatesList();
                            for (HotelProtos.KeyValuePair_DateTime_PriceDate priceDate : priceDates) {
                                for (HotelProtos.Price price : priceDate.getValue().getPricesList()) {
                                    sb.append(String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
                                            ycsHotel.getID(), ycsHotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn, priceInfo.getRatePlanID(), price.getType().name(), price.getApplyTo(), price.getOccupancy(), 0, price.getValue()));
                                }
                            }
                        }
                    }
                }

                if (sb.length() > 0) {
                    path = "out/maha/" + checkIn + "-" + content + "-ycs.txt";
                    FileSystem hdfs = FileSystem.get(new Configuration());
                    bw = new BufferedWriter( new OutputStreamWriter( hdfs.create( new Path(path), true ), "UTF-8" ) );
                    bw.write(sb.toString());
                    bw.flush();
                    bw.close();
                    hdfs.close();
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        request();

    }

    protected void shutdown() {
        shutdownLatch.countDown();
    }

}

public class Tester {

    public static void main(String[] args) {

        int concurrency, subsetSize;
        if (args.length == 2) {
            concurrency = Integer.parseInt(args[0]);
            subsetSize = Integer.parseInt(args[1]);
        } else if (args.length == 1){
            concurrency = Integer.parseInt(args[0]);
            subsetSize = -1;
        } else {
            concurrency = 50;
            subsetSize = -1;
        }

        List<Integer> hotelIds;
        if (subsetSize > 0)
            hotelIds = SQLHelper.getAllHotelIds(new String[]{}).subList(0, subsetSize);
        else
            hotelIds = SQLHelper.getAllHotelIds(new String[]{});
        int startOffset = Integer.parseInt(args[3]), stopOffset = Integer.parseInt(args[4]);

        DateTimeFormatter format = DateTimeFormat.forPattern("yyyyMMdd");

        DateTime searchDate = format.parseDateTime(args[2]);
        DateTime searchDateJoda = new DateTime(searchDate.getYear(), searchDate.getMonthOfYear(), searchDate.getDayOfMonth(), 0, 0, 0, DateTimeZone.forOffsetHours(7));
        DateTime checkIn = null;

        final int ITERATIONS = hotelIds.size() / concurrency;
        List<Consumer> consumers = new ArrayList<>(concurrency);

        final ResourceBundle config = ResourceBundle.getBundle("config");
        Session session = getSession(config.getString("cassandra.load.hosts").split(","));
        session.execute("USE " + config.getString("cassandra.load.keyspace"));
        final String QUERY = "SELECT * FROM ratesilo WHERE hotelid = ? AND checkin = ? AND losbucket = 0 AND los = 1";
        PreparedStatement stmt = session.prepare(QUERY);

        for (int dayOffset = startOffset; dayOffset <= stopOffset; dayOffset++) {
            checkIn = searchDateJoda.plusDays(dayOffset);

            int fromIndex, toIndex;
            for (int i = 0; i < concurrency; i++) {
                fromIndex = i * ITERATIONS;
                toIndex = (fromIndex + ITERATIONS) > hotelIds.size() ?
                        hotelIds.size() :
                        (fromIndex + ITERATIONS);
                consumers.add(new DemoConsumer(session, new ArrayList<Integer>(hotelIds.subList(fromIndex, toIndex)), stmt, checkIn.getMillis()));
            }

            System.out.println("Starting threads for " + checkIn.getMillis());
            for (Consumer consumer : consumers)
                consumer.start();
            for (Consumer consumer : consumers)
                consumer.join();

            System.out.println("Threads Done for " + checkIn.getMillis());

            // Clearing consumers
            consumers.clear();
        }
        System.exit(0);
    }

    public static Session getSession(String[] hosts) {
        int maxConnections = 2;

        PoolingOptions pools = new PoolingOptions();
        pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);
        Cluster cluster = new Cluster.Builder()
                .addContactPoints("sg-agdfcas-6b01",
                        "sg-agdfcas-6b02",
                        "sg-agdfcas-6b03",
                        "sg-agdfcas-6b04",
                        "sg-agdfcas-6b05",
                        "sg-agdfcas-6b06",
                        "sg-agdfcas-6b07",
                        "sg-agdfcas-6b08")
                .withPoolingOptions(pools)
                .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withSocketOptions(new SocketOptions().setTcpNoDelay(true))
                .build();

        return cluster.connect();
    }

}
