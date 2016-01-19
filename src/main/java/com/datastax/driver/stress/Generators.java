package com.datastax.driver.stress;

/**
 * Created by malam on 1/5/16.
 */

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.utils.Bytes;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class Generators {
    private static ThreadLocal<Random> random = new ThreadLocal<Random>() {
        @Override
        protected Random initialValue() {
            return new Random();
        }
    };

    private static ByteBuffer makeValue(final int valueSize) {
        byte[] value = new byte[valueSize];
        random.get().nextBytes(value);
        return ByteBuffer.wrap(value);
    }

    private static abstract class AbstractGenerator extends QueryGenerator {
        protected int iteration;

        protected AbstractGenerator(int iterations) {
            super(iterations);
        }

        @Override
        public int currentIteration() {
            return iteration;
        }

        public boolean hasNext() {
            return iterations == -1 || iteration < iterations;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static final QueryGenerator.Builder INSERTER = new QueryGenerator.Builder() {

        public String name() {
            return "insert";
        }

        public OptionParser addOptions(OptionParser parser) {
            String msg = "Simple insertion of CQL3 rows (using prepared statements unless the --no-prepare option is used). "
                    + "The inserted rows have a fixed set of columns but no clustering columns.";
            parser.formatHelpWith(Stress.Help.formatFor(name(), msg));

            parser.accepts("no-prepare", "Do no use prepared statement");
            parser.accepts("columns-per-row", "Number of columns per CQL3 row").withRequiredArg().ofType(Integer.class).defaultsTo(5);
            parser.accepts("value-size", "The size in bytes for column values").withRequiredArg().ofType(Integer.class).defaultsTo(34);
            parser.accepts("with-compact-storage", "Use COMPACT STORAGE on the table used");
            return parser;
        }

        public void prepare(OptionSet options, Session session) {

            try {
                session.execute("DROP KEYSPACE stress;");
            } catch (QueryValidationException e) { /* Fine, ignore */ }

            session.execute("CREATE KEYSPACE stress WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

            session.execute("USE stress");

            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE standard1 (key bigint PRIMARY KEY");
            for (int i = 0; i < (Integer) options.valueOf("columns-per-row"); ++i)
                sb.append(", C").append(i).append(" blob");
            sb.append(')');

            if (options.has("with-compact-storage"))
                sb.append(" WITH COMPACT STORAGE");

            session.execute(sb.toString());
        }

        public QueryGenerator create(int id, List<Integer> hotelIds, Long checkIn, int iterations, OptionSet options, Session session, PreparedStatement statement) {

            return options.has("no-prepare")
                    ? createRegular(id, iterations, options)
                    : createPrepared(id, iterations, options, session);
        }

        public QueryGenerator createRegular(int id, int iterations, OptionSet options) {
            final int valueSize = (Integer) options.valueOf("value-size");
            final int columnsPerRow = (Integer) options.valueOf("columns-per-row");
            final long prefix = (long) id << 32;

            return new AbstractGenerator(iterations) {
                public QueryGenerator.Request next() {
                    StringBuilder sb = new StringBuilder();
                    sb.append("UPDATE standard1 SET ");
                    for (int i = 0; i < columnsPerRow; ++i) {
                        if (i > 0) sb.append(", ");
                        sb.append('C').append(i).append("='").append(Bytes.toHexString(makeValue(valueSize))).append('\'');
                    }
                    sb.append(" WHERE key = ").append(prefix | iteration);
                    ++iteration;
                    return new QueryGenerator.Request.SimpleQuery(new SimpleStatement(sb.toString()));
                }
            };
        }

        public QueryGenerator createPrepared(int id, int iterations, OptionSet options, Session session) {
            final int valueSize = (Integer) options.valueOf("value-size");
            final int columnsPerRow = (Integer) options.valueOf("columns-per-row");
            final long prefix = (long) id << 32;

            StringBuilder sb = new StringBuilder();
            sb.append("UPDATE standard1 SET ");
            for (int i = 0; i < columnsPerRow; ++i) {
                if (i > 0) sb.append(", ");
                sb.append('C').append(i).append("=?");
            }
            sb.append(" WHERE key = ?");

            final PreparedStatement stmt = session.prepare(sb.toString());

            return new AbstractGenerator(iterations) {
                public QueryGenerator.Request next() {
                    BoundStatement b = stmt.bind();
                    b.setLong("key", prefix | iteration);
                    for (int i = 0; i < columnsPerRow; ++i)
                        b.setBytes("c" + i, makeValue(valueSize));
                    ++iteration;
                    return new QueryGenerator.Request.PreparedQuery(b);
                }
            };
        }
    };

    public static final QueryGenerator.Builder READER = new QueryGenerator.Builder() {

        public String name() {
            return "read";
        }

        public OptionParser addOptions(OptionParser parser) {
            String msg = "Read the rows inserted with the insert generator. Use prepared statements unless the --no-prepare option is used.";
            parser.formatHelpWith(Stress.Help.formatFor(name(), msg));

            parser.accepts("no-prepare", "Do no use prepared statement");
            return parser;
        }

        public void prepare(OptionSet options, Session session) {
            KeyspaceMetadata ks = session.getCluster().getMetadata().getKeyspace("dragonfruitqa2");
            if (ks == null || ks.getTable("ratesilo") == null) {
                System.err.println("There is nothing to reads, please run insert/insert_prepared first.");
                System.exit(1);
            }

            session.execute("USE dragonfruitqa2");
        }

        public QueryGenerator create(int id, List<Integer> hotelIds, Long checkIn, int iterations, OptionSet options, Session session, PreparedStatement statement) {
            return options.has("no-prepare")
                    ? createRegular(id, iterations)
                    : createPrepared(id, hotelIds, checkIn, iterations, session, statement);
        }

        public QueryGenerator createRegular(long id, int iterations) {
            final long prefix = (long) id << 32;
            return new AbstractGenerator(iterations) {
                public QueryGenerator.Request next() {
                    StringBuilder sb = new StringBuilder();
                    sb.append("SELECT * FROM standard1 WHERE key = ").append(prefix | iteration);
                    ++iteration;
                    return new QueryGenerator.Request.SimpleQuery(new SimpleStatement(sb.toString()));
                }
            };
        }

        public QueryGenerator createPrepared(long id, List<Integer> hotelIds, final Long checkIn, int iterations, Session session, PreparedStatement statement) {
            final long prefix = (long) id << 32;
//            final PreparedStatement stmt = session.prepare("SELECT * FROM standard1 WHERE key = ?");

            final PreparedStatement stmt = statement;
            final List<Integer> hotels = new ArrayList<Integer>(hotelIds);

            return new AbstractGenerator(iterations) {
                public QueryGenerator.Request next() {
                    BoundStatement bs = stmt.bind();
//                    System.out.print(hotels.get(iteration));
//                    bs.setLong("key", prefix | iteration);
                    bs.setInt("hotelid", hotels.get(iteration));
                    bs.setDate("checkin", new Date(checkIn));
                    ++iteration;
                    return new QueryGenerator.Request.PreparedQuery(bs);
                }
            };
        }
    };
}