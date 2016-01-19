package com.datastax.driver.stress;

import agoda.dragonfruit.helper.HotelHelper;
import agoda.dragonfruit.helper.SQLHelper;
import agoda.dragonfruit.helper.SupplierHelper;
import agoda.search.models.protobuf.HotelProtos;
import agoda.search.models.protobuf.Suppliers;
import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * Created by malam on 1/6/16.
 */

public class Test {

    private final BlockingQueue<Runnable> queue;
    private final ExecutorService executorService;
    private final List<Integer> hotelIds;
    private final Session session;
    private static final String QUERY = "SELECT * FROM ratesilo WHERE hotelid = ? AND checkin = ? AND losbucket = 0 AND los = 1";
    final PreparedStatement stmt;
    private static Logger logger = Logger.getLogger(Test.class.getName());
    private final int MAX_SIZE = 10000;
    private static int cassResults = 0;
    private static int ycsCounter = 0;
    private static int dmcCounter = 0;
//    private static FileSystem hdfs;
    private static List<Long> stats;
    private static int subsetSize = -1;
//    private static ConcurrentHashMap<Integer, String> hotels;
//    private final StringBuilder sb = new StringBuilder();

    public List getHotelIds() {
        return this.hotelIds;
    }

    public Test(Session session, int threadNum) {
        this.session = session;
        session.execute("USE dragonfruit");
        stmt = session.prepare(QUERY);
        hotelIds = new ArrayList<Integer>();
        stats = new ArrayList<Long>();
        queue = new ArrayBlockingQueue<>(MAX_SIZE);
//        hotels = new ConcurrentHashMap<Integer, String>();

        int workers;
        if (threadNum > 0)
            workers = threadNum;
        else
            workers = Runtime.getRuntime().availableProcessors();
        executorService = new ThreadPoolExecutor(workers,
                workers,
                0L,
                TimeUnit.MILLISECONDS, queue);
        /*try {
            hdfs = FileSystem.get(new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }

    private synchronized void applyHotelLogic(HotelProtos.Hotel hotel, long checkIn, FSDataOutputStream priceWriter) throws IOException {
        this.ycsCounter++;
//        BufferedWriter priceWriter = null;
        StringBuilder sb = new StringBuilder();

//            priceWriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path("out/test/ycs/" + checkIn + "-" + hotel.getID() + ".txt"), true), "UTF-8"));
            List<HotelProtos.PriceInfo> priceInfos = hotel.getPriceInfosList();
            for (HotelProtos.PriceInfo priceInfo : priceInfos) {
                for (HotelProtos.KeyValuePair_Int32_RateCategory rate : priceInfo.getRateCategoriesList()) {
                    HotelProtos.RateCategory rateCategory = rate.getValue();
                    for (HotelProtos.KeyValuePair_DateTime_PriceDate priceDate : rateCategory.getDatesList()) {
                        HotelProtos.PriceDate price = priceDate.getValue();
                        for (HotelProtos.Price p : price.getPricesList()) {
//                            this.hotels.put(hotel.getID(), String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
//                                    hotel.getID(), hotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn, priceInfo.getRatePlanID(), p.getType().name(), p.getApplyTo(), p.getOccupancy(), 0, p.getValue()));
                            sb.append(String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
                                    hotel.getID(), hotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn, priceInfo.getRatePlanID(), p.getType().name(), p.getApplyTo(), p.getOccupancy(), 0, p.getValue()));
                        }
                    }
                }

                if (hotel.getSupplierID() != 332) {
                    for (HotelProtos.RateMatrix rateMatrix : priceInfo.getMatrixesList()) {
//                        this.hotels.put(hotel.getID(), String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
//                                hotel.getID(), hotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn, priceInfo.getRatePlanID(), "Room", "PRPN", rateMatrix.getAdults(), rateMatrix.getChildren(), rateMatrix.getTotalRate()));
                        sb.append(String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
                                hotel.getID(), hotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn, priceInfo.getRatePlanID(), "Room", "PRPN", rateMatrix.getAdults(), rateMatrix.getChildren(), rateMatrix.getTotalRate()));
                    }
                } else {
                    List<HotelProtos.KeyValuePair_DateTime_PriceDate> priceDates = priceInfo.getDatesList();
                    for (HotelProtos.KeyValuePair_DateTime_PriceDate priceDate : priceDates) {
                        for (HotelProtos.Price price : priceDate.getValue().getPricesList()) {
//                            this.hotels.put(hotel.getID(), String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
//                                    hotel.getID(), hotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn, priceInfo.getRatePlanID(), price.getType().name(), price.getApplyTo(), price.getOccupancy(), 0, price.getValue()));
                            sb.append(String.format("%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d\t%d\t%f\n",
                                    hotel.getID(), hotel.getSupplierID(), priceInfo.getRoomTypeID(), checkIn, priceInfo.getRatePlanID(), price.getType().name(), price.getApplyTo(), price.getOccupancy(), 0, price.getValue()));
                        }
                    }
                }
            }

//            priceWriter.flush();
            byte[] byt = sb.toString().getBytes();
            priceWriter.write(byt);
    }

    private synchronized void applySupplierLogic(Suppliers.SupplierHotel hotel, long checkIn, FSDataOutputStream supplierPriceWriter) throws IOException {
//        this.sb.append(hotel.getAgodaHotelID()).append(",");
        this.dmcCounter++;
        StringBuilder sb = new StringBuilder();

            for (Suppliers.SupplierPriceInfo price : hotel.getSupplierPriceInfosList()) {
//                this.hotels.put(hotel.getAgodaHotelID(), String.format(
//                        "%d\t%d\t%d\t%d\t%d\t%d\t%s\t%f\n",
//                        hotel.getAgodaHotelID(), price.getDmcID(), price.getAgodaRoomTypeID(), checkIn, price.getAgodaRatePlanID(), price.getOccupancy(), price.getCurrencyCode(), price.getSellIn()));
                sb.append(String.format(
                        "%d\t%d\t%d\t%d\t%d\t%d\t%s\t%f\n",
                        hotel.getAgodaHotelID(), price.getDmcID(), price.getAgodaRoomTypeID(), checkIn, price.getAgodaRatePlanID(), price.getOccupancy(), price.getCurrencyCode(), price.getSellIn()));
            }

//            supplierPriceWriter.flush();
            byte[] byt = sb.toString().getBytes();
            supplierPriceWriter.write(byt);
    }

    public void doSomething(int hotelId, long checkIn) throws InterruptedException, IOException {
        long start = System.nanoTime();
//        System.out.println(Thread.currentThread().getName() + ": HotelID " + hotelId);

        BoundStatement bs = stmt.bind();
        bs.setInt("hotelid", hotelId);
        bs.setDate("checkin", new Date(checkIn));
        ResultSet rs = null;
            rs = session.execute(bs);

        Row row = rs.one();
        if (row != null) {
            cassResults++;
//            logger.info("Got response: " + row.toString());

            // Checking for Supplier Hotel
            Suppliers.SupplierHotel sHotel = null;
            int hotelID = -1;
            for(int supplierId: new int[]{3038,27900}) {
                sHotel = SupplierHelper.parseCsRow(row, supplierId);
                if(sHotel != null) {
                    hotelID = sHotel.getAgodaHotelID();
                    break;
                }
            }

            FileSystem hdfs = FileSystem.get(new Configuration());
            FSDataOutputStream priceWriter = hdfs.create(new Path("out/test/ycs/" + checkIn + "-" + hotelId + ".txt"));
            if(sHotel != null && hotelID != -1) {
                applySupplierLogic(sHotel, checkIn, priceWriter);
            } else {
                applyHotelLogic(HotelHelper.parseCsRow(row), checkIn, priceWriter);
            }
            priceWriter.close();
            hdfs.close();
        } else {
            logger.info(hotelId + "-" + checkIn + ": No response");
        }
        long finish = System.nanoTime();
        stats.add(finish-start);
//        logger.info(String.format("hotelId : %s took %.3f ms", hotelId, (finish-start)/1e6));
    }

    public void start() throws InterruptedException {
        for (int i=0; i<hotelIds.subList(0, subsetSize).size(); i++) {
            final int hotelId = hotelIds.get(i);
            try {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            doSomething(hotelId, 1453222800000L);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                while(queue.size() == MAX_SIZE) {
                    Thread.sleep(1000);
                    i--;
                }
            }
        }
    }

    public void shutdown() {
        this.executorService.shutdown();
    }

    public static void main(String[] args) {

        System.out.println("Number of Threads: " + Runtime.getRuntime().availableProcessors());

        Test test = null;
        if (args.length > 1 && args[1] != null)
            test = new Test(getSession(), Integer.parseInt(args[1]));
        else
            test = new Test(getSession(), -1);

        test.hotelIds.addAll(SQLHelper.getAllHotelIds(new String[]{}));
        if (args[0].isEmpty())
            subsetSize = test.hotelIds.size();
        else
            subsetSize = Integer.parseInt(args[0]);
        int total = subsetSize;
//        int total = 10000;

        long start = System.nanoTime();
        try {
            test.start();
            test.shutdown();
            boolean done = test.executorService.awaitTermination(1, TimeUnit.MINUTES);
            while (!done) {
                done = test.executorService.awaitTermination(1, TimeUnit.MINUTES);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long finish = System.nanoTime();
        long sum = 0;
        for (Long time : stats) {
            sum += time;
        }

        /*StringBuilder sbHotels = new StringBuilder();
        for (Integer hotelId : test.hotels.keySet()) {
            sbHotels.append(hotelId).append(",");
        }*/
        System.out.println("============= STATS =============");
        System.out.println(String.format("Average thread execution time : %.3f", sum / (total * 1e6)));
//        logger.info(String.format("Map size : %s", sbHotels.toString()));
        System.out.println(String.format("Number of processed hotels : %d", cassResults));
        System.out.println(String.format("Number of ycs hotels : %d", ycsCounter));
        System.out.println(String.format("Number of dmc hotels : %d", dmcCounter));
        System.out.println(String.format("Number of missed hotels : %d", total - cassResults));
        System.out.println(String.format("Total time taken : %.3f", (finish - start) / 1e9));
//        logger.info(String.format("DMC hotels : %s", test.sb.toString()));

        System.exit(0);
    }

    public static Session getSession() {
        final int concurency = 10;
        int maxConnections = 2;

        PoolingOptions pools = new PoolingOptions();
        pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);
        Cluster cluster = new Cluster.Builder()
                .addContactPoints(
                        "sg-agdfcas-6b01",
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
