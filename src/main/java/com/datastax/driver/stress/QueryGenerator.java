package com.datastax.driver.stress;

/**
 * Created by malam on 1/5/16.
 */
import com.datastax.driver.core.*;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.util.Iterator;
import java.util.List;

public abstract class QueryGenerator implements Iterator<QueryGenerator.Request> {

    protected final int iterations;

    protected QueryGenerator(int iterations) {
        this.iterations = iterations;
    }

    public abstract int currentIteration();

    public int totalIterations() {
        return iterations;
    }

    public interface Builder {
        public String name();

        public OptionParser addOptions(OptionParser parser);

        public void prepare(OptionSet options, Session session);

        public QueryGenerator create(int id, List<Integer> hotelIds, Long chekcIn, int iterations, OptionSet options, Session session, PreparedStatement statement);
    }

    public interface Request {

        public ResultSet execute(Session session);

        public ResultSetFuture executeAsync(Session session);

        public static class SimpleQuery implements Request {

            private final Statement statement;

            public SimpleQuery(Statement statement) {
                this.statement = statement;
            }

            public ResultSet execute(Session session) {
                return session.execute(statement);
            }

            public ResultSetFuture executeAsync(Session session) {
                return session.executeAsync(statement);
            }
        }

        public static class PreparedQuery implements Request {

            private final BoundStatement query;

            public PreparedQuery(BoundStatement query) {
                this.query = query;
            }

            public ResultSet execute(Session session) {
                return session.execute(query);
            }

            public ResultSetFuture executeAsync(Session session) {
                return session.executeAsync(query);
            }
        }
    }
}