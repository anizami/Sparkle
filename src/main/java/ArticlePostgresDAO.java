/**
 * Created by Asra Nizami on 3/4/14.
 */

import java.sql.*;
import java.util.ArrayList;

public class ArticlePostgresDAO<T extends Article> implements ArticleDbService<T> {

    // PostgreSQL connection to the database
    private Connection conn;
    // A raw SQL query used without parameters
    private Statement stmt;

    public ArticlePostgresDAO() {
        // The account names setup from the command line interface
        String user = "postgres";
        String passwd = "1600grand";
        String dbName = "sparkledb";
        // DB connection on localhost via JDBC
        String uri = "jdbc:postgresql://localhost/" + dbName;

        // Standard SQL Create Table Query

        String createTableQuery =
                "CREATE TABLE IF NOT EXISTS article( " +
                        "id         INT             PRIMARY KEY NOT NULL," +
                        "title      VARCHAR(64)     NOT NULL," +
                        "content    VARCHAR(512)    NOT NULL," +
                        "summary    VARCHAR(64)     NOT NULL," +
                        "deleted    BOOLEAN         DEFAULT FALSE," +
                        "createdAt  DATE            NOT NULL" +
                        ");"
                ;
        try {
          conn = DriverManager.getConnection(uri, user, passwd);
          stmt = conn.createStatement();
          stmt.execute(createTableQuery);
          System.out.println("Connecting to PSQL Database");
        } catch(Exception e) {
            System.out.println(e.getMessage());
            System.out.println("This is the eleventh exception");

            try {
                if(null != stmt) {
                    stmt.close();
                }
                if(null != conn) {
                    conn.close();
                }
            }   catch(SQLException sqlException) {
                sqlException.printStackTrace();
                System.out.println("This is the twelveth exception");
            }
        }

    }

    @Override
    public Boolean create(T entity) {
        try {
            String insertQuery = "INSERT INTO article(id, title, content, summary, createdAt) VALUES(?, ?, ?, ?, ?);";
            // Prepared statements allow us to avoid SQL injection attacks
            PreparedStatement pstmt = conn.prepareStatement(insertQuery);

            // JDBC binds every prepared statement argument to a Java Class such as Integer and or String
            pstmt.setInt(1, entity.getId());
            pstmt.setString(2, entity.getTitle());
            pstmt.setString(3, entity.getContent());
            pstmt.setString(4, entity.getSummary());

            java.sql.Date sqlNow = new Date(new java.util.Date().getTime());
            pstmt.setDate(5, sqlNow);

            pstmt.executeUpdate();
            // Unless closed prepared statement connections will linger
            // Not very important for a trivial app but it will burn you in a professional large codebase
            pstmt.close();

            return true;

        } catch(SQLException e) {
            System.out.println(e.getMessage());
            System.out.println("This is the first exception");

            try {
                if(null != stmt) {
                    stmt.close();
                }
                if(null != conn) {
                    conn.close();
                }
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                System.out.println("This is the second exception");
            }

            return false;

        }
    }


        public T readOne(int id){
            try {
                String selectQuery = "SELECT * FROM article where id = ?";

                PreparedStatement pstmt = conn.prepareStatement(selectQuery);
                pstmt.setInt(1, id);

                pstmt.executeQuery();

                // A ResultSet is Class which represents a table returned by a SQL query
                ResultSet resultSet = pstmt.getResultSet();
                if(resultSet.next()) {
                    Article entity = new Article(
                            // You must know both the column name and the type to extract the row
                            (String) resultSet.getString("title"),
                            (String) resultSet.getString("summary"),
                            (String) resultSet.getString("content"),
                            (Integer) resultSet.getInt("id")
                    );
                    pstmt.close();
                    return (T) entity;


                }
            } catch(Exception e) {
                System.out.println(e.getMessage());
                System.out.println("This is the third exception");

                try {
                    if(null != stmt) {
                        stmt.close();
                    }
                    if(null != conn) {
                        conn.close();
                    }
                } catch (SQLException sqlException) {
                    sqlException.printStackTrace();
                    System.out.println("This is the fourth exception");
                }
            }

            return null;
        }

    public ArrayList<T> readAll() {
// Type cast the generic T into an Article
        ArrayList<Article> results = (ArrayList<Article>) new ArrayList<T>();

        try {
            String query = "SELECT * FROM article;";

            stmt.execute(query);
            ResultSet resultSet = stmt.getResultSet();

            while(resultSet.next()) {
                Article entity = new Article(
                        resultSet.getString("title"),
                        resultSet.getString("summary"),
                        resultSet.getString("content"),
                        resultSet.getInt("id")
                );

                results.add(entity);
            }
        } catch(Exception e) {
            System.out.println(e.getMessage());
            System.out.println("This is the fifth exception");

            try {
                if(null != stmt) {
                    stmt.close();
                }
                if(null != conn) {
                    conn.close();
                }
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                System.out.println("This is the sixth exception");
            }
        }

        // The interface ArticleDbService relies upon the generic type T so we cast it back
        return (ArrayList<T>) results;
    }

    @Override
    public Boolean update(int id, String title, String summary, String content) {
        try {
            String updateQuery =
                    "UPDATE article SET title = ?, summary = ?, content = ?" +
                            "WHERE id = ?;"
                    ;

            PreparedStatement pstmt = conn.prepareStatement(updateQuery);
            pstmt.setString(1, title);
            pstmt.setString(2, summary);
            pstmt.setString(3, content);
            pstmt.setInt(4, id);

            pstmt.executeUpdate();
        } catch(Exception e) {
            System.out.println(e.getMessage());
            System.out.println("This is the seventh exception");

            try {
                if(null != stmt) {
                    stmt.close();
                }
                if(null != conn) {
                    conn.close();
                }
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                System.out.println("This is the eighth exception");
            }
        }

        return true;
    }
    @Override
    public Boolean delete(int id) {
        try {
            String deleteQuery = "DELETE FROM article WHERE id = ?";

            PreparedStatement pstmt = conn.prepareStatement(deleteQuery);
            pstmt.setInt(1, id);

            pstmt.executeUpdate();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println("This is the ninth exception");

            try {
                if(null != stmt) {
                    stmt.close();
                }
                if(null != conn) {
                    conn.close();
                }
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                System.out.println("This is the tenth exception");
            }
        }

        return true;
    }

}
