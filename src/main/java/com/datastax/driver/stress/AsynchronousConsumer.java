package com.datastax.driver.stress;

/**
 * Created by malam on 1/5/16.
 */

import agoda.dragonfruit.helper.HotelHelper;
import agoda.dragonfruit.helper.SupplierHelper;
import agoda.search.models.protobuf.HotelProtos;
import agoda.search.models.protobuf.Suppliers;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;

public class AsynchronousConsumer implements Consumer {

    private static final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final Session session;
    private final QueryGenerator requests;
//    private final Reporter reporter;

    private final HashMap<Integer, HotelProtos.Hotel> hotels = new HashMap<Integer, HotelProtos.Hotel>();
    private final HashMap<Integer, List<Suppliers.SupplierHotel>> supplierHotels = new HashMap<Integer, List<Suppliers.SupplierHotel>>();

    public Map<Integer, HotelProtos.Hotel> getHotels() {
        return this.hotels;
    }

    public Map<Integer, List<Suppliers.SupplierHotel>> getSupplierHotels() {
        return this.supplierHotels;
    }

    public AsynchronousConsumer(Session session,
                                QueryGenerator requests) {
        this.session = session;
        this.requests = requests;
//        this.reporter = reporter;
    }

    public void start() {
        executorService.execute(new Runnable() {
            public void run() {
                request();
            }
        });
    }

    private void request() {

        if (!requests.hasNext()) {
            shutdown();
            return;
        }

        handle(requests.next());
    }

    public void join() {
        awaitUninterruptibly(shutdownLatch);
    }

    protected void handle(QueryGenerator.Request request) {

//        final Reporter.Context ctx = reporter.newRequest();

        ResultSetFuture resultSetFuture = request.executeAsync(session);
        final StringBuilder sb = new StringBuilder();
        Futures.addCallback(resultSetFuture, new FutureCallback<ResultSet>() {
            public void onSuccess(final ResultSet result) {
                int[] supplierIds = new int[]{3038, 27900};

                for (Row row : result.all()) {
                    HotelProtos.Hotel hotel = HotelHelper.parseCsRow(row);
                    if(hotel != null) {
                        hotels.put(hotel.getID(), hotel);
                    }

                    // Suppliers Part
                    ArrayList<Suppliers.SupplierHotel> sHotels = new ArrayList<Suppliers.SupplierHotel>();
                    int hotelId = -1;
                    for(int supplierId: supplierIds) {
                        Suppliers.SupplierHotel sHotel = SupplierHelper.parseCsRow(row, supplierId);
                        if(sHotel != null) {
                            hotelId = sHotel.getAgodaHotelID();
                            sHotels.add(sHotel);
                        }
                    }
                    if(sHotels.size() > 0 && hotelId != -1) {
                        supplierHotels.put(hotelId, sHotels);
                    }
                }

//                ctx.done();
                request();
            }

            public void onFailure(final Throwable t) {
                // Could do better I suppose
                System.err.println("Error during request: " + t);
//                ctx.done();
                request();
            }
        }, executorService);
    }

    protected void shutdown() {
        shutdownLatch.countDown();
    }
}