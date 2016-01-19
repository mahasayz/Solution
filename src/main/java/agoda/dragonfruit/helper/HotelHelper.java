package agoda.dragonfruit.helper;

import agoda.search.models.protobuf.HotelProtos.*;
import com.datastax.driver.core.Row;
import com.google.protobuf.InvalidProtocolBufferException;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by conman on 3/12/15.
 */
public final class HotelHelper {

    private static final LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();
    private static Logger logger = Logger.getLogger(HotelHelper.class.getName());

    private static long convertToUTC(long epoch) throws ParseException {
        Date checkInDate = new Date(epoch * 1000);
        return checkInDate.getTime();
    }

    public static String correctingDate(String date) {
        StringBuilder start = new StringBuilder(date);
        String[] temp = start.toString().split("T");
        start.setLength(0);
        start.append(temp[0] + " " + temp[1]);
        return start.toString();
    }

    /*public static Map<Integer, ArrayList<Long>> selectCORUpdate(List tBuckets, long from, long to) {
        String query = "SELECT tuuid, data" +
                " FROM calculation_history" +
                " WHERE tbucket in (" + StringUtils.join(tBuckets, ",") + ")" +
                " AND tuuid >= minTimeuuid(%s)" +
                " AND tuuid <= minTimeuuid(%s)" +
                " ALLOW FILTERING;";
        String cql = String.format(query, from, to);

        System.out.println("Query: " + cql);

        SimpleStatement statement = new SimpleStatement(cql);
        Cassandra cass = CQLHelper.Cassandra();

        if(cass == null) {
        }

        ResultSet resultSet = cass.execute(statement);
        List<String> jsonResult = new ArrayList<String>();
        for(Row row: resultSet.all()) {
            jsonResult.add(row.getString("data"));
        }

        JsonParser parser = new JsonParser();
        JsonObject jsonObject = null;
        int hotelID;
        Date start, end;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Map<Integer, ArrayList<Long>> map = new HashMap<>();
        ArrayList<Long> temp = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, 180);
        calendar.set(Calendar.HOUR_OF_DAY, 00);
        calendar.set(Calendar.MINUTE, 00);
        calendar.set(Calendar.SECOND, 00);

        for (String json : jsonResult) {
            try {
                jsonObject = parser.parse(json).getAsJsonObject();
                hotelID = jsonObject.get("HotelID").getAsInt();
                start = sdf.parse(correctingDate(jsonObject.get("Start").getAsString()));
                end = sdf.parse(correctingDate(jsonObject.get("End").getAsString()));
                if (map.get(hotelID) == null) {
                    temp.add(start.getTime());
                    if (end.after(calendar.getTime()))
                        temp.add(calendar.getTime().getTime());
                    else
                        temp.add(end.getTime());
                    map.put(hotelID, new ArrayList<Long>(temp));
                } else {
                    temp = map.get(hotelID);
                    if (temp.get(0) > start.getTime()) {
                        temp.remove(0);
                        temp.add(0, start.getTime());
                    }
                    if (temp.get(1) < end.getTime()) {
                        temp.remove(1);
                        if (end.after(calendar.getTime()))
                            temp.add(1, calendar.getTime().getTime());
                        else
                            temp.add(1, end.getTime());
                    }
                    map.put(hotelID, new ArrayList<Long>(temp));
                }
                temp.clear();
                System.out.println(json);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    public static CORUpdateData getCORUpdate(long from, long to) {
        int bucketPeriod = 300;
        long tBucketStart = from - (from % bucketPeriod);
        long tBucketEnd = to - (to % bucketPeriod);
        long count = (tBucketEnd - tBucketStart) / bucketPeriod;
        List<Long> tBuckets = new ArrayList<Long>();

        for(int i=0; i<count; i++) {
            tBuckets.add(tBucketStart + i * bucketPeriod);
        }

        Map<Integer, ArrayList<Long>> result = new HashMap<>();
        Date earliest = null, latest = null;
        try {
            result = selectCORUpdate(tBuckets, convertToUTC(from), convertToUTC(to));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date start, end;
            Calendar cal = Calendar.getInstance();
            for (Integer hotelId : result.keySet()) {
                start = new Date(result.get(hotelId).get(0));
                end = new Date(result.get(hotelId).get(1));
                System.out.println(hotelId + " : " + sdf.format(start) + " - " + sdf.format(end));
                cal.setTime(start);
                if (earliest == null) {
                    earliest = cal.getTime();
                } else {
                    if (earliest.after(start)) {
                        earliest = cal.getTime();
                    }
                }
                cal.setTime(end);
                if (latest == null) {
                    latest = cal.getTime();
                } else {
                    if (latest.before(end)) {
                        latest = cal.getTime();
                    }
                }
            }
            System.out.println("Earliest : " + sdf.format(earliest) + ", Latest : " + sdf.format(latest));
        } catch (ParseException e) {
            System.out.println(e.getMessage());
        }
        return new CORUpdateData(result, earliest, latest);
    }*/

    public static Map<Integer, Double> cheapestPrice(Hotel hotel) {
        Map<Integer, Double> cheapest = new HashMap<Integer, Double>();

        if(hotel == null) {
            return cheapest;
        }

        List<PriceInfo> priceInfos =  hotel.getPriceInfosList();
        Integer[] ratePlans = getUniqueRatePlan(priceInfos);

        for(Integer ratePlan : ratePlans) {

            double cheapestForRatePlan = Double.POSITIVE_INFINITY;
            for(PriceInfo priceInfo : priceInfos) {

                List<KeyValuePair_DateTime_PriceDate> priceDates = priceInfo.getDatesList();
                Long[] dates = getUniqueDates(priceDates);

                double totalPrice = 0.0;
                for(Long date : dates) {
                    double cheapestForDate = Double.POSITIVE_INFINITY;
                    for (KeyValuePair_DateTime_PriceDate priceDate : priceDates) {
                        // check date
                        if(priceDate.getKey().getValue() == date) {
                            for(Price price : priceDate.getValue().getPricesList()) {
                                if(price.getType() == ChargeType.Room
                                        && price.getValue() < cheapestForDate) {
                                    cheapestForDate = price.getValue();
                                }
                            }
                        }
                    }

                    totalPrice += cheapestForDate;
                }

                if(totalPrice < cheapestForRatePlan) {
                    cheapestForRatePlan = totalPrice;
                }
            }
            cheapest.put(ratePlan, cheapestForRatePlan);
        }
        return cheapest;
    }

    public static int getLOSBucket(int los) {
        int ret = 0;
        if (los < 4) {
            ret = 0;
        } else if (los < 8) {
            ret = 1;
        } else {
            ret = 2;
        }
        return ret;
    }

    public static Hotel parseCsRow(Row row) {
        if(row == null) {
            return null;
        }

        int _hotelId = row.getInt("hotelid");
        Date _checkin = row.getDate("checkin");
        int _los = row.getInt("los");

        ByteBuffer b = row.getBytes("pricedata");

        if(b == null) {
            return null;
        }

        try {
            byte[] bytes = new byte[b.remaining()];
            b.get(bytes);

            if (bytes[0] != 34) {
                byte[] origSizeBytes = new byte[4];
                origSizeBytes[0] = bytes[1];
                origSizeBytes[1] = bytes[2];
                origSizeBytes[2] = bytes[3];
                origSizeBytes[3] = bytes[4];
                int origSize = ByteBuffer.wrap(origSizeBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

                byte[] compSizeBytes = new byte[4];
                compSizeBytes[0] = bytes[5];
                compSizeBytes[1] = bytes[6];
                compSizeBytes[2] = bytes[7];
                compSizeBytes[3] = bytes[8];
                int compSize = ByteBuffer.wrap(compSizeBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();

                byte[] decompressed = new byte[origSize];
                decompressor.decompress(bytes, 9, compSize, decompressed, 0);

                Hotel hotel = Hotel.parseFrom(decompressed);

                return hotel;
            } else {
                bytes = java.util.Arrays.copyOfRange(bytes, 1, bytes.length);

                Hotel hotel = Hotel.parseFrom(bytes);

                return hotel;
            }
        }
        catch(InvalidProtocolBufferException ipbEx) {
            return null;
        }
    }

    private static Long[] getUniqueDates(List<KeyValuePair_DateTime_PriceDate> priceDates) {
        Set<Long> dates = new HashSet<Long>();
        for(KeyValuePair_DateTime_PriceDate priceDate : priceDates) {
            dates.add(priceDate.getKey().getValue());
        }
        Long[] dateArray = dates.toArray(new Long[dates.size()]);
        Arrays.sort(dateArray);
        return dateArray;
    }

    private static Integer[] getUniqueRatePlan(List<PriceInfo> priceInfos) {
        Set<Integer> rates = new HashSet<Integer>();
        for(PriceInfo priceInfo : priceInfos) {
            rates.add(priceInfo.getRatePlanID());
        }
        Integer[] rateArray = rates.toArray(new Integer[rates.size()]);
        Arrays.sort(rateArray);
        return rateArray;
    }

}
