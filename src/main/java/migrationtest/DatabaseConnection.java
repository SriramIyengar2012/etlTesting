package migrationtest;

import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.sql.Connection;
import java.sql.DriverManager;

public class DatabaseConnection {

    public static JdbcIO.DataSourceConfiguration sourceConnection(){
        JdbcIO.DataSourceConfiguration config  = JdbcIO.DataSourceConfiguration.create(
                "org.apache.derby.jdbc.ClientDriver", "jdbc:derby://localhost:1527//Users/sriramiyengar/Documents/derby/db-derby-10.12.1.1-bin/bin/MyDbTest");

        return config;

    }

    public static Connection targetConnection(){
        Connection conn = null;
        try {
           conn = DriverManager.getConnection("url", "username", "password");
        }
        catch(Exception e) {
             e.printStackTrace();
        }
        return conn;
    }

}
