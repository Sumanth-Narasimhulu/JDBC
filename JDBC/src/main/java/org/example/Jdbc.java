package org.example;

import java.sql.*;

public class Jdbc {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/demo";
        String username = "postgres";
        String password = "0000";

        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            // Insert data into tables
            insertData(conn);
            // Update data
            updateData(conn);
            // Delete data
            deleteData(conn);
            // Retrieve data
            retrieveData(conn);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    // Method to retrieve and display data using a JOIN query
    private static void retrieveData(Connection conn) throws SQLException {
        String joinSQL = "SELECT e.emp_id, e.emp_name, d.dept_name " +
                "FROM employees e " +
                "JOIN departments d ON e.dept_id = d.dept_id";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(joinSQL)) {
            System.out.println("Join Query Results:");
            while (rs.next()) {
                int empId = rs.getInt("emp_id");
                String empName = rs.getString("emp_name");
                String deptName = rs.getString("dept_name");
                System.out.println("Employee ID: " + empId + ", Name: " + empName + ", Department: " + deptName);
            }
        }
    }

    // Method to delete an employee by name
    private static void deleteData(Connection conn) throws SQLException {
        String sql = "DELETE FROM employees WHERE emp_name = ?";
        try (PreparedStatement dstmt = conn.prepareStatement(sql)) {
            dstmt.setString(1, "Shanthi");
            int rows = dstmt.executeUpdate();
            System.out.println(rows + " row(s) affected after deleting.");
            // Optionally, retrieve updated data
            retrieveData(conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Method to update an employee's name
    private static void updateData(Connection conn) throws SQLException {
        String sql = "UPDATE employees SET emp_name = ? WHERE emp_name = ?";
        try (PreparedStatement ustmt = conn.prepareStatement(sql)) {
            ustmt.setString(1, "Shanthi");
            ustmt.setString(2, "Syamala");
            int rows = ustmt.executeUpdate();
            System.out.println(rows + " row(s) affected and updated.");
            // Optionally, retrieve updated data
            retrieveData(conn);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Method to insert data into departments and employees tables
    private static void insertData(Connection conn) throws SQLException {
        String deptSql = "INSERT INTO departments(dept_name) VALUES(?)";
        // Use RETURN_GENERATED_KEYS to capture auto-generated dept_id
        try (PreparedStatement deptstmt = conn.prepareStatement(deptSql, Statement.RETURN_GENERATED_KEYS)) {
            deptstmt.setString(1, "Sales");
            deptstmt.executeUpdate();
            ResultSet deptKeys = deptstmt.getGeneratedKeys();
            int deptId = 0;
            if (deptKeys.next()) {
                deptId = deptKeys.getInt(1);
            }
            deptKeys.close();

            String empSql = "INSERT INTO employees(emp_name, dept_id) VALUES(?, ?)";
            try (PreparedStatement empstmt = conn.prepareStatement(empSql)) {
                // Insert first employee
                empstmt.setString(1, "Sumanth");
                empstmt.setInt(2, deptId);
                empstmt.executeUpdate();

                // Clear parameters before inserting the second employee
                empstmt.clearParameters();
                empstmt.setString(1, "Syamala");
                empstmt.setInt(2, deptId);
                empstmt.executeUpdate();

                System.out.println("Data inserted successfully.");
                // Optionally, retrieve inserted data
                retrieveData(conn);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
