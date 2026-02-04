import java.sql.*;
import java.util.*;

/**
 * Baseline client that runs the workload against a single database.
 * Used to generate expected_output.txt for accuracy comparison.
 */
public class SingleDbClient {

    private Connection conn;

    public void setupConnection() {
        try {
            Class.forName("org.postgresql.Driver");
            String url = "jdbc:postgresql://localhost:5432/single_db";
            conn = DriverManager.getConnection(url, "user", "password");
            System.out.println("Connected to single_db");
        } catch (Exception e) {
            System.out.println("Connection to single_db failed");
            e.printStackTrace();
        }
    }

    public void insertStudent(String studentId, String name, int age, String email) {
        try {
            String sql = "INSERT INTO Student (student_id, name, age, email) VALUES (?, ?, ?, ?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, studentId);
            pstmt.setString(2, name);
            pstmt.setInt(3, age);
            pstmt.setString(4, email);
            pstmt.executeUpdate();
            pstmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void insertGrade(String studentId, String courseId, int score) {
        try {
            String sql = "INSERT INTO Grade (student_id, course_id, score) VALUES (?, ?, ?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, studentId);
            pstmt.setString(2, courseId);
            pstmt.setInt(3, score);
            pstmt.executeUpdate();
            pstmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateGrade(String studentId, String courseId, int newScore) {
        try {
            String sql = "UPDATE Grade SET score = ? WHERE student_id = ? AND course_id = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, newScore);
            pstmt.setString(2, studentId);
            pstmt.setString(3, courseId);
            pstmt.executeUpdate();
            pstmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteStudentFromCourse(String studentId, String courseId) {
        try {
            String sql = "DELETE FROM Grade WHERE student_id = ? AND course_id = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, studentId);
            pstmt.setString(2, courseId);
            pstmt.executeUpdate();
            pstmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getStudentProfile(String studentId) {
        try {
            String sql = "SELECT name, email FROM Student WHERE student_id = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, studentId);
            ResultSet rs = pstmt.executeQuery();

            String result = null;
            if (rs.next()) {
                String name = rs.getString("name");
                String email = rs.getString("email");
                result = name + "," + email;
            }

            rs.close();
            pstmt.close();
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public String getAvgScoreByDept() {
        try {
            String sql =
                    "SELECT c.department AS dept, AVG(g.score) AS avg_score " +
                    "FROM Grade g JOIN Course c ON g.course_id = c.course_id " +
                    "GROUP BY c.department";

            PreparedStatement pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            Map<String, Double> deptAvg = new HashMap<>();
            while (rs.next()) {
                String dept = rs.getString("dept");
                double avg = rs.getDouble("avg_score");
                deptAvg.put(dept, avg);
            }

            rs.close();
            pstmt.close();

            if (deptAvg.isEmpty()) {
                return "";
            }

            List<String> departments = new ArrayList<>(deptAvg.keySet());
            Collections.sort(departments);

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < departments.size(); i++) {
                String dept = departments.get(i);
                double avg = deptAvg.get(dept);
                if (i > 0) sb.append(";");
                sb.append(dept)
                  .append(":")
                  .append(String.format(java.util.Locale.US, "%.1f", avg));
            }

            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public String getAllStudentsWithMostCourses() {
        try {
            String sql =
                    "SELECT student_id, COUNT(*) AS cnt " +
                    "FROM Grade " +
                    "GROUP BY student_id";

            PreparedStatement pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            int maxCnt = 0;
            List<String> topStudents = new ArrayList<>();

            while (rs.next()) {
                String studentId = rs.getString("student_id");
                int cnt = rs.getInt("cnt");

                if (cnt > maxCnt) {
                    maxCnt = cnt;
                    topStudents.clear();
                    topStudents.add(studentId);
                } else if (cnt == maxCnt && cnt > 0) {
                    topStudents.add(studentId);
                }
            }

            rs.close();
            pstmt.close();

            if (topStudents.isEmpty() || maxCnt == 0) {
                return "";
            }

            Collections.sort(topStudents);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < topStudents.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(topStudents.get(i));
            }

            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public void closeConnection() {
        try {
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
            System.out.println("Single DB connection closed successfully");
        } catch (SQLException e) {
            System.out.println("Error closing single DB connection");
            e.printStackTrace();
        }
    }
}

