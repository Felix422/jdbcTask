package org.example;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;
import java.util.Scanner;

public class App {
    private static Statement stmt;
    private static boolean isQuit;
    private static final int wdt = 2;

    public static void main(String[] args ) throws SQLException {
        init();     // Инициализируем соединение с БД
        run();      // Запускаем цикл запросов
        close();    // Закрываем соединение с БД
    }

    private static void init() throws SQLException {
        // Получаем данные из файла config.properties
        Properties prop = new Properties();
        String jdbcUrl;
        String username;
        String password;

        try (InputStream input = App.class.getClassLoader().getResourceAsStream("config.properties")) {
            prop.load(input);
            jdbcUrl = prop.getProperty("db.jdbcUrl");
            username = prop.getProperty("db.username");
            password = prop.getProperty("db.password");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Устанавливаем соединение с БД
        Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
        stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        System.out.println("Подключение установлено! \nВведите SQL выражение или QUIT для выхода:");
    }

    private static void run() {
        Scanner scanner = new Scanner(System.in);
        isQuit = false;

        while (!isQuit) {
            // Получаем ввод пользователя
            System.out.print("\n>");
            String userInput = scanner.nextLine();

            String[] arr = userInput.split(" ");
            String firstWord = arr[0];
            if ("QUIT".equalsIgnoreCase(userInput)) {             // Обрабатываем выход из программы
                quit();
            } else if ("SELECT".equalsIgnoreCase(firstWord)) {    // Отдельно обрабатываем запросы типа SELECT
                selectRequest(userInput);                         // и выводим не более 10 записей
            } else {
                defaultRequest(userInput);                        // Остальные запросы обрабатываем по умолчанию
            }
        }
    }

    private static void quit(){
        isQuit = true;
    }

    private static void defaultRequest(String userInput){
        try{
            stmt.executeUpdate(userInput);
            System.out.println("Запрос выполнен.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }

    private static void selectRequest(String userInput){
        countOfRows(userInput);     // Считаем кол-во записей по запросу
        printResultSet(userInput);  // Выводим данные по запросу так, чтобы выводились не более 10 записей
    }

    private static void countOfRows(String userInput)  {
        try {
            // Считаем кол-во записей по запросу
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM(" + userInput + ")");
            int count = 0;
            if(rs.next()){
                count = rs.getInt(1);
            }

            // Если кол-во записей больше 10, то выводим первые 10 записей
            if (count > 10) {
                System.out.println("Выведены первые 10 записей по вашему запросу. Осталось записей В БД: " + (count - 10));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void printResultSet(String userInput){
        try {
            // Получаем метаданные таблицы и вычисляем максимальную длину столбцов
            ResultSet rs = stmt.executeQuery(userInput + " LIMIT 10");
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            int[] columnWidths = new int[columnCount];
            for (int i = 1; i <= columnCount; i++) {
                columnWidths[i - 1] = metaData.getColumnName(i).length();
            }


            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String columnValue = rs.getString(i);
                    if (columnValue != null && columnValue.length() > columnWidths[i - 1]) {
                        columnWidths[i - 1] = columnValue.length();
                    }
                }
            }
            rs.beforeFirst();

            // Выводим заголовки столбцов
            printSeparator(columnCount, columnWidths);
            System.out.print("|");
            for (int i = 1; i <= columnCount; i++) {
                System.out.printf("%-" + (columnWidths[i - 1] + wdt) + "s|", metaData.getColumnName(i));
            }
            System.out.println();

            printSeparator(columnCount, columnWidths);

            // Выводим данные по запросу
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String columnValue = rs.getString(i);
                    System.out.printf("|%-" + (columnWidths[i - 1] + wdt-1) + "s ", columnValue != null ? columnValue : "NULL");
                }
                System.out.println("|");
            }
            printSeparator(columnCount, columnWidths);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void printSeparator(int columnCount, int[] columnWidths){
        System.out.print("+");
        for (int i = 1; i <= columnCount; i++) {
            System.out.print("=".repeat(columnWidths[i - 1] + wdt)+"+");
        }
        System.out.println();
    }

    private static void close() throws SQLException {
        stmt.close();
        System.out.println("Соединение закрыто.");
    }
}
