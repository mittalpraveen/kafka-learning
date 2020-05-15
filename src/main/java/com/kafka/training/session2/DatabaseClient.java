package com.kafka.training.session2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class DatabaseClient {

    private static Connection connection;
    Logger logger = LoggerFactory.getLogger(DatabaseClient.class);

    public DatabaseClient() {
        init();
    }

    private void init() {
        try {
            // This will load the MySQL driver, each DB has its own driver
            Class.forName("com.mysql.jdbc.Driver");
            // Setup the connection with the DB
            connection = DriverManager
                    .getConnection("jdbc:mysql://localhost/local_test?"
                            + "user=local_user&password=password123");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void persistRecordData(String recordId, String recordData) throws SQLException {
        PreparedStatement ps = null;
        try {
            connection.setAutoCommit(false);
            String sql = "insert into local_test.record values (default, ?, ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, recordId);
            ps.setString(2, recordData);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    public void persistPartitionOffset(int partition, long offset, String topic) throws SQLException {
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        try {
            connection.setAutoCommit(false);

            ps = connection
                    .prepareStatement("SELECT partition_offset, topic_partition, topic from local_test.PARTITION_OFFSET_DATA where topic_partition=? and topic =? ");
            ps.setInt(1, partition);
            ps.setString(2, topic);
            resultSet = ps.executeQuery();
            if (resultSet.next() == false) {
                String sql = "insert into local_test.PARTITION_OFFSET_DATA values (?, ?, ?)";
                ps = connection.prepareStatement(sql);
                ps.setInt(1, partition);
                ps.setLong(2, offset);
                ps.setString(3, topic);
                ps.executeUpdate();
            } else {
                String sql = "update local_test.PARTITION_OFFSET_DATA set partition_offset=? where topic_partition = ? and topic = ?";
                ps = connection.prepareStatement(sql);
                ps.setLong(1, offset);
                ps.setInt(2, partition);
                ps.setString(3, topic);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                ps.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }

    public void commitTransaction() {
        try {
            connection.setAutoCommit(false);
            connection.commit();
        } catch (SQLException e) {
            if (connection != null) {
                try {
                    System.err.print("Transaction is being rolled back");
                    connection.rollback();
                } catch (SQLException excep) {
                    excep.printStackTrace();
                }
            }
        }
    }

    public long getOffsetForPartition(int partition, String topic) throws SQLException {
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        long offset = 0;
        try {
            ps = connection
                    .prepareStatement("SELECT partition_offset from local_test.PARTITION_OFFSET_DATA where topic_partition = ? and topic = ?");
            ps.setInt(1, partition);
            ps.setString(2, topic);
            resultSet = ps.executeQuery();
            if (resultSet.next() == false) {
                logger.error("No offset found");
//                throw new IllegalArgumentException();
            } else {
                do {
                    offset = resultSet.getLong("partition_offset");
                } while (resultSet.next());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null)
                resultSet.close();
        }
        return offset;
    }
}
