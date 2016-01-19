package agoda.dragonfruit.helper;

import org.apache.commons.lang.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.ResourceBundle;
import java.util.logging.Logger;

/**
 * Created by conman on 3/11/15.
 */
public final class SQLHelper {

    private static final ResourceBundle config = ResourceBundle.getBundle("config");
    private static boolean isInitialized = false;
    private static final Logger logger = Logger.getLogger(SQLHelper.class.getName());

    private static final String connDriver = config.getString("sqlserver.driver");

    private static final String connYcsString = config.getString("sqlserver.ycs.connectionString");
    private static final String connCoreString = config.getString("sqlserver.core.connectionString");
    private static final String connCoreWriterString = config.getString("sqlserver.core.writer.connectionString");
    private static final String connHiveString = config.getString("sqlserver.hive.connectionString");

    private static final String connYcsUser = config.getString("sqlserver.ycs.username");
    private static final String connCoreUser = config.getString("sqlserver.core.username");
    private static final String connCoreWriterUser = config.getString("sqlserver.core.writer.username");
    private static final String connHiveUser = config.getString("sqlserver.hive.username");

    private static final String connYcsPass = config.getString("sqlserver.ycs.password");
    private static final String connCorePass = config.getString("sqlserver.core.password");
    private static final String connCoreWriterPass = config.getString("sqlserver.core.writer.password");
    private static final String connHivePass = config.getString("sqlserver.hive.password");

    private static Connection connYcs = null;
    private static Connection connCore = null;
    private static Connection connCoreWriter = null;
    private static Connection connHive = null;

    public static void init() {
        logger.info("Initializing SQLHelper...");
        if (isInitialized)
            return;

        DriverManager.setLoginTimeout(2);

        try {
            Class.forName(connDriver);
            connYcs = DriverManager.getConnection(connYcsString, connYcsUser, connYcsPass);
        } catch (ClassNotFoundException ex) {
            logger.warning("Cannot initialize YCS SQL db connection");
            //isInitialized = false;
        } catch (SQLException ex) {
            logger.warning("Cannot initialize YCS SQL db connection");
        }

        try {
            Class.forName(connDriver);
            connCoreWriter = DriverManager.getConnection(connCoreWriterString, connCoreWriterUser, connCoreWriterPass);
        } catch (ClassNotFoundException ex) {
            logger.warning("Cannot initialize CORE/Writer SQL db connection");
            //isInitialized = false;
        } catch (SQLException ex) {
            logger.warning("Cannot initialize CORE/Writer SQL db connection");
            //isInitialized = false;
        }

        try {
            Class.forName(connDriver);
            connCore = DriverManager.getConnection(connCoreString, connCoreUser, connCorePass);
        } catch (ClassNotFoundException ex) {
            logger.warning("Cannot initialize CORE SQL db connection");
            isInitialized = false;
            return;
        }  catch (SQLException ex) {
            logger.warning("Cannot initialize CORE SQL db connection");
            isInitialized = false;
            return;
        }

        try {
            Class.forName(connDriver);
            connHive = DriverManager.getConnection(connHiveString, connHiveUser, connHivePass);
        } catch (ClassNotFoundException ex) {
            logger.warning("Cannot initialize CORE SQL db connection");
            isInitialized = false;
            return;
        } catch (SQLException ex) {
            logger.warning("Cannot initialize CORE SQL db connection");
            isInitialized = false;
            return;
        }

        isInitialized = true;
    }

    public static ArrayList<Integer> getAllHotelIds(String[] cityIds) {
        ArrayList<Integer> hotelIds = new ArrayList<Integer>();
        if (!isInitialized)
            init();
        try {
            String cond = "1 = 1";
            if (cityIds.length > 0) {
                if (!cityIds[0].isEmpty()) {
                    cond = "[city_id] IN (" + StringUtils.join(cityIds, ",") + ")";
                }
            }
            Statement statement = connCore.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT DISTINCT hotel_id\n" +
                    "  FROM [Agoda_Core].[dbo].[product_hotels]" +
                    "  WHERE " + cond);
            while (resultSet.next()) {
                hotelIds.add(resultSet.getInt("hotel_id"));
            }
            return hotelIds;
        } catch (SQLException sqlException) {
            return null;
        }
    }

    public static ArrayList<Integer> getAllCityIds() {
        ArrayList<Integer> cityIds = new ArrayList<Integer>();
        if (!isInitialized) {
            init();
        }
        try {
            logger.info("Getting list of cityIds");
            Statement statement = connCore.createStatement();
            ResultSet resultSet = statement.executeQuery("SELECT DISTINCT city_id\n" +
                    "  FROM [Agoda_Core].[dbo].[product_hotels]");
            while (resultSet.next()) {
                cityIds.add(resultSet.getInt("city_id"));
            }
            logger.info("Getting list of cityIds done. Found " + cityIds.size() + " cities.");
            return cityIds;
        } catch (SQLException sqlException) {
            logger.severe(sqlException.getMessage());
            return null;
        }
    }

}
