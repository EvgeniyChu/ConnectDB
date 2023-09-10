import java.sql.*;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

 public class Main {

    public static void main(String[] args) {
//Есть база данных UXCrowd. Адрес - lt.uxcrowd.ru:54322
// база данных: uxtest, логин - student, пароль - f8rndk
        Connection connection = null;
        //URL к базе состоит из протокола:подпротокола://[хоста]:[порта_СУБД]/[БД] и других_сведений
        String url = "jdbc:postgresql://lt.uxcrowd.ru:54322/uxtest";
        //Имя пользователя БД
        String name = "student";
        //Пароль
        String password = "f8rndk";

        String CSV_FILE = "D:\\test.csv";

        Date date = new Date();

        try {
            //Загружаем драйвер
            Class.forName("org.postgresql.Driver");
            System.out.println("Драйвер подключен");
            //Создаём соединение
            connection = DriverManager.getConnection(url, name, password);
            System.out.println("Соединение установлено");
            //Для использования SQL запросов существуют 3 типа объектов:
            //1.Statement: используется для простых случаев без параметров
            Statement statement = null;

            statement = connection.createStatement();

            BufferedWriter writer = Files.newBufferedWriter(Paths.get(CSV_FILE));

            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                    .withHeader("Datetime", "Query", "Mean_time"));

            //Выполним запрос
            ResultSet result1 = statement.executeQuery(
                    "select * from pg_stat_statements where calls>10 order by mean_time desc limit 25");
            //result это указатель на первую строку с выборки
            //чтобы вывести данные мы будем использовать
            //метод next() , с помощью которого переходим к следующему элементу
            System.out.println("Выводим statement");
            while (result1.next()) {
                csvPrinter.printRecord(date,result1.getString("query"), result1.getDouble("mean_time"));
                csvPrinter.flush();
                System.out.println("Query (" + result1.getString("query")+ ")" + "\tMean_time " + result1.getDouble("mean_time"));
            }
        } catch (Exception ex) {
            //выводим наиболее значимые сообщения
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }

    }
}