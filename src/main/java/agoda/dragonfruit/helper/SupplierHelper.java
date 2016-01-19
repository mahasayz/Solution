package agoda.dragonfruit.helper;

import agoda.search.models.protobuf.Suppliers;
import agoda.search.models.protobuf.Suppliers.SupplierHotel;
import com.datastax.driver.core.Row;
import com.google.protobuf.InvalidProtocolBufferException;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Date;

/**
 * Created by conman on 5/22/15.
 */
public class SupplierHelper {

    private static final LZ4SafeDecompressor decompressor = LZ4Factory.fastestInstance().safeDecompressor();

    private static final int[] supplierIds = new int[]{3038, 27900};


    public static SupplierHotel parseCsRow(Row row, int supplierId) {
        if(row == null) {
            return null;
        }

        int _hotelId = row.getInt("hotelid");
        Date _checkin = row.getDate("checkin");
        int _los = row.getInt("los");

        ByteBuffer b = row.getBytes("price" + supplierId);

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

                SupplierHotel sHotel = Suppliers.SupplierHotel.parseFrom(decompressed);

                return sHotel;
            } else {
                bytes = java.util.Arrays.copyOfRange(bytes, 1, bytes.length);

                SupplierHotel sHotel = SupplierHotel.parseFrom(bytes);

                return sHotel;
            }
        }
        catch(InvalidProtocolBufferException ipbEx) {
            return null;
        }
    }

    // Supplier -> SellIn
    public static Double cheapestPrice(SupplierHotel hotel) {
        double cheapest = Double.POSITIVE_INFINITY;

        for(Suppliers.SupplierPriceInfo price: hotel.getSupplierPriceInfosList()) {
            if(price.getSellIn() < cheapest) {
                cheapest = price.getSellIn();
            }
        }

        return cheapest;
    }

}
