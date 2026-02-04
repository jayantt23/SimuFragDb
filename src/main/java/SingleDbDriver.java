import java.io.*;
import java.util.Scanner;

/**
 * Driver that runs the workload against a single database using SingleDbClient.
 * Produces expected_output.txt for baseline accuracy comparison.
 */
public class SingleDbDriver {

    static {
        java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("Asia/Kolkata"));
    }

    public static void main(String[] args) {
        SingleDbClient client = new SingleDbClient();

        try {
            System.out.println("Initializing single DB connection...");
            client.setupConnection();

            InputStream in = SingleDbDriver.class
                    .getClassLoader()
                    .getResourceAsStream("workload.txt");

            if (in == null) {
                System.err.println("Error: workload.txt not found on classpath.");
                return;
            }

            Scanner scanner = new Scanner(in);
            PrintWriter outputWriter = new PrintWriter("expected_output.txt");

            System.out.println("Processing workload on single DB...");
            long startTime = System.currentTimeMillis();

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",");
                String command = parts[0];

                try {
                    switch (command) {
                        case "INSERT_STUDENT":
                            client.insertStudent(
                                    parts[1], parts[2],
                                    Integer.parseInt(parts[3]), parts[4]);
                            break;

                        case "INSERT_GRADE":
                            client.insertGrade(
                                    parts[1], parts[2],
                                    Integer.parseInt(parts[3]));
                            break;

                        case "UPDATE_GRADE":
                            client.updateGrade(
                                    parts[1], parts[2],
                                    Integer.parseInt(parts[3]));
                            break;

                        case "DELETE_STUDENT_COURSE":
                            client.deleteStudentFromCourse(parts[1], parts[2]);
                            break;

                        case "READ_PROFILE":
                            String profile = client.getStudentProfile(parts[1]);
                            outputWriter.println(profile != null ? profile : "NULL");
                            break;

                        case "READ_SCORE":
                            String gpa = client.getAvgScoreByDept();
                            outputWriter.println(gpa != null ? gpa : "NULL");
                            break;

                        case "READ_ALL":
                            String top = client.getAllStudentsWithMostCourses();
                            outputWriter.println(top != null ? top : "NULL");
                            break;

                        default:
                            outputWriter.println("ERROR: Unknown command " + command);
                    }
                } catch (Exception e) {
                    outputWriter.println("ERROR: " + e.getMessage());
                }
            }

            long endTime = System.currentTimeMillis();
            System.out.println("Single DB workload finished in " + (endTime - startTime) + "ms");

            scanner.close();
            outputWriter.close();
            client.closeConnection();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

