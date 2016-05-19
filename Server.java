package server;

/**
 *
 * @author Alexandru
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import static java.lang.Math.PI;
import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;
import java.sql.SQLException;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

public class Server implements Runnable {

    Socket clientSocket;

    Server(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    public static void main(String args[])
            throws Exception {

        ServerSocket serverSocket = new ServerSocket(63400);
        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(new Server(socket)).start();
        }
    }

    private static double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private static double rad2deg(double rad) {
        return (rad * 180 / Math.PI);
    }

    private static double distance(double lat1, double lon1, double lat2, double lon2, String unit) {
        double theta = lon1 - lon2;
        double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2)) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        dist = dist * 60 * 1.1515;
        if ("K".equals(unit)) {
            dist = dist * 1.609344;
        } else if ("N".equals(unit)) {
            dist = dist * 0.8684;
        }

        return (dist);
    }

    @Override
    public void run() {

        try {
            BufferedReader read = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
            PrintWriter write = new PrintWriter(this.clientSocket.getOutputStream(), true);

            String option;

            System.out.println(Thread.currentThread().getId() + " is connected.");

            try {
                Class.forName("oracle.jdbc.driver.OracleDriver");
            } catch (ClassNotFoundException e) {
                System.out.println("Where is your Oracle JDBC Driver?");
                return;
            }
            Connection connection;
            try {
                connection = DriverManager.getConnection(
                        "jdbc:oracle:thin:@Ratusca:1521:XE", "student",
                        "student");
            } catch (SQLException e) {
                System.out.println("Connection Failed!");
                return;
            }
            if (connection == null) {
                write.println("Failed to make connection to database!");
            }

            option = read.readLine();

            while (!"exit".equals(option)) {
                switch (option) {

                    case "login": {

                        String username = read.readLine();
                        String password = read.readLine();

                        try {

                            CallableStatement checkData = connection.prepareCall("{call rapi.loginDataValidation(?,?,?,?)}");

                            checkData.setString(1, username);
                            checkData.setString(2, password);

                            checkData.registerOutParameter(3, java.sql.Types.VARCHAR);
                            checkData.registerOutParameter(4, java.sql.Types.NUMERIC);

                            checkData.execute();

                            String status = checkData.getString(3);

                            write.flush();
                            write.println(status);

                        } catch (SQLException e) {
                            System.out.println(e);
                        }
                    }
                    break;

                    case "register": {

                        String lastName = read.readLine();
                        String foreName = read.readLine();
                        String userName = read.readLine();
                        String password = read.readLine();
                        String email = read.readLine();
                        int phoneNumber = Integer.parseInt(read.readLine());

                        CallableStatement insertUser;
                        String procedure = "{call rapi.insertUserData(?,?,?,?,?,?,?,?)}";

                        try {

                            insertUser = connection.prepareCall(procedure);
                            insertUser.setString(1, userName);
                            insertUser.setString(2, password);
                            insertUser.setString(3, lastName);
                            insertUser.setString(4, foreName);
                            insertUser.setString(5, email);
                            insertUser.setInt(6, phoneNumber);

                            insertUser.registerOutParameter(7, java.sql.Types.VARCHAR);
                            insertUser.registerOutParameter(8, java.sql.Types.NUMERIC);

                            insertUser.execute();

                            String status = insertUser.getString(7);

                            write.println(status);

                        } catch (SQLException e) {
                            System.out.println(e);
                        }
                    }
                    break;

                    case "location": {

                        double latitude = Double.parseDouble(read.readLine());
                        double longitude = Double.parseDouble(read.readLine());
                        double radius = Double.parseDouble(read.readLine());

                        Statement statement;
                        try {

                            statement = connection.createStatement();
                            ResultSet result = statement.executeQuery("select * from rapi_buildings");
                            while (result.next()) {
                                double lat = result.getDouble("latitude");
                                double lon = result.getDouble("longitude");
                                String link = result.getString("picture");
                                double d = distance(latitude, longitude, lat, lon, "K");
                                if (radius >= d * 1000) {
                                    write.println(lat + " " + lon + " " + link);
                                }
                            }
                            write.println("");
                        } catch (SQLException e) {
                            System.out.println(e);
                        }
                    }
                    break;

                    case "insertData": {

                        String dataJSON = read.readLine();
                        JSONObject insertData = (JSONObject) JSONSerializer.toJSON(dataJSON);

                        String city = (String) insertData.get("city");
                        String street_name = (String) insertData.get("street_name");
                        String street_number = (String) insertData.get("street_number");
                        String block = (String) insertData.get("block");
                        String suite = (String) insertData.get("suite");
                        String username = (String) insertData.get("username");
                        String picture = (String) insertData.get("picture");
                        String latitude = (String) insertData.get("latitude");
                        String longitude = (String) insertData.get("longitude");

                        CallableStatement insertBuildingData;
                        String procedure = "{call rapi.insertBuildingData(?,?,?,?,?,?,?,?,?,?,?)}";

                        try {

                            int street_no = Integer.parseInt(street_number);
                            int lat = Integer.parseInt(latitude);
                            int lon = Integer.parseInt(longitude);

                            insertBuildingData = connection.prepareCall(procedure);
                            insertBuildingData.setString(1, city);
                            insertBuildingData.setString(2, street_name);
                            insertBuildingData.setInt(3, street_no);
                            insertBuildingData.setString(4, block);
                            insertBuildingData.setString(5, suite);
                            insertBuildingData.setString(6, username);
                            insertBuildingData.setString(7, picture);
                            insertBuildingData.setInt(8, lat);
                            insertBuildingData.setInt(9, lon);

                            insertBuildingData.registerOutParameter(10, java.sql.Types.VARCHAR);
                            insertBuildingData.registerOutParameter(11, java.sql.Types.NUMERIC);

                            insertBuildingData.execute();

                            String status = insertBuildingData.getString(10);

                            write.println(status);

                        } catch (SQLException e) {
                            System.out.println("Error!");
                        }
                    }
                    break;
                }
                option = read.readLine();
            }
            clientSocket.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
