package fragment;
import java.sql.*;
import java.util.*;
public class FragmentClient {

    private Map<Integer, Connection> connectionPool;
    private Router router;
    private int numFragments;

    public FragmentClient(int numFragments) {
        this.numFragments = numFragments;
        this.router = new Router(numFragments);
        this.connectionPool = new HashMap<>();
    }

    /**
     * TODO: Initialize JDBC connections to all N Fragments.
     */
    public void setupConnections() {
        try {
            Class.forName("org.postgresql.Driver");

            for (int i = 0; i < numFragments; i++) {
                String url = "jdbc:postgresql://localhost:5432/frag_" + i;

                Connection conn = DriverManager.getConnection(url, "user", "password");
                connectionPool.put(i, conn);
                System.out.println("Connected to frag_" + i);
            }
        } catch (Exception e) {
            System.out.println("Connection failed");
            e.printStackTrace();
        }
    }

    /**
     * TODO: Route the student to the correct shard and execute the INSERT.
     */
    public void insertStudent(String studentId, String name, int age, String email) {
        try {
            // Route to the correct fragment based on studentId
            int fragmentId = router.getFragmentId(studentId);
            
            // Get the connection for this fragment
            Connection conn = connectionPool.get(fragmentId);
            
            // Prepare the INSERT statement
            String sql = "INSERT INTO Student  (student_id, name, age, email) VALUES (?, ?, ?, ?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            
            // Set parameters
            pstmt.setString(1, studentId);
            pstmt.setString(2, name);
            pstmt.setInt(3, age);
            pstmt.setString(4, email);
            
            // Execute the insert
            pstmt.executeUpdate();
            
            // Close the prepared statement
            pstmt.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO: Route the grade to the correct shard and execute the INSERT.
     */
    public void insertGrade(String studentId, String courseId, int score) {
        try {
            // Route to the correct fragment based on studentId
            int fragmentId = router.getFragmentId(studentId);
            
            // Get the connection for this fragment
            Connection conn = connectionPool.get(fragmentId);
            
            // Prepare the INSERT statement
            String sql = "INSERT INTO Grade (student_id, course_id, score) VALUES (?, ?, ?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            
            // Set parameters
            pstmt.setString(1, studentId);
            pstmt.setString(2, courseId);
            pstmt.setInt(3, score);
            
            // Execute the insert
            pstmt.executeUpdate();
            
            // Close the prepared statement
            pstmt.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void updateGrade(String studentId, String courseId, int newScore) {
        try {
            // Route to the correct fragment based on studentId
            int fragmentId = router.getFragmentId(studentId);
            
            // Get the connection for this fragment
            Connection conn = connectionPool.get(fragmentId);
            
            // Prepare the UPDATE statement
            String sql = "UPDATE Grade SET score = ? WHERE student_id = ? AND course_id = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            
            // Set parameters
            pstmt.setInt(1, newScore);
            pstmt.setString(2, studentId);
            pstmt.setString(3, courseId);
            
            // Execute the update
            pstmt.executeUpdate();
            
            // Close the prepared statement
            pstmt.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteStudentFromCourse(String studentId, String courseId) {
        try {
            // Route to the correct fragment based on studentId
            int fragmentId = router.getFragmentId(studentId);
            
            // Get the connection for this fragment
            Connection conn = connectionPool.get(fragmentId);
            
            // Prepare the DELETE statement
            String sql = "DELETE FROM Grade WHERE student_id = ? AND course_id = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            
            // Set parameters
            pstmt.setString(1, studentId);
            pstmt.setString(2, courseId);
            
            // Execute the delete
            pstmt.executeUpdate();
            
            // Close the prepared statement
            pstmt.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * TODO: Fetch the student's name and email.
     */
    public String getStudentProfile(String studentId) {
        try {
            // Route to the correct fragment based on studentId
            int fragmentId = router.getFragmentId(studentId);
            
            // Get the connection for this fragment
            Connection conn = connectionPool.get(fragmentId);
            
            // Prepare the SELECT statement
            String sql = "SELECT name, email FROM Student WHERE student_id = ?";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            
            // Set parameter
            pstmt.setString(1, studentId);
            
            // Execute the query
            ResultSet rs = pstmt.executeQuery();
            
            String result = null;
            if (rs.next()) {
                String name = rs.getString("name");
                String email = rs.getString("email");
                result = name + "," + email;
            }
            
            // Close resources
            rs.close();
            pstmt.close();
            
            return result;
            
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    /**
     * TODO: Calculate the average score per department.
     */
    public String getAvgScoreByDept() {
        try {
            // Your code here
            return null;

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    /**
     * TODO: Find all the students that have taken most number of courses
     */
    public String getAllStudentsWithMostCourses() {
        try {
            // Your code here
            return null;

        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    public void closeConnections() {
        try {
            // Close all connections in the connection pool
            for (Connection conn : connectionPool.values()) {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            }
            System.out.println("All connections closed successfully");
        } catch (SQLException e) {
            System.out.println("Error closing connections");
            e.printStackTrace();
        }
    }
}
