import java.sql.Statement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLite {
    private Connection conexion;

    public void abrirConexion(String bd, String servidor, String usuario,
            String password) {
        try {
            String url = String.format("jdbc:mysql://localhost:3306/add?useServerPrepStmts=true", "usuario",
                    "contraseña");

            this.conexion = DriverManager.getConnection(url, usuario, password);
            if (this.conexion != null) {
                System.out.println("Conectado a " + bd + " en " + servidor);
            } else {
                System.out.println("No conectado a " + bd + " en " + servidor);
            }
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getLocalizedMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("Código error: " + e.getErrorCode());
        }
    }

    public void abrirConexionSQLite(String bd2) {
        try {
            String url = "jdbc:sqlite:" + bd2;

            this.conexion = DriverManager.getConnection(url);
            if (this.conexion != null) {
                System.out.println("Conectado a " + bd2);
            } else {
                System.out.println("No conectado a " + bd2);
            }
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getLocalizedMessage());
            System.out.println("SQLState: " + e.getSQLState());
            System.out.println("Código error: " + e.getErrorCode());
        }
    }

    public void cerrarConexion() {
        try {
            this.conexion.close();
        } catch (SQLException e) {
            System.out.println("Error al cerrar la conexión: " + e.getLocalizedMessage());
        }
    }

    public void ej1() throws SQLException {
        String query = "show create table alumnos";


        try (Statement stmt = this.conexion.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                System.out.println(rs.getString(2));
            }
        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }

    }


    public static void main(String[] args) {
        SQLite ej = new SQLite();

        ej.abrirConexion("add", "localhost", "root", "");
        //ej.abrirConexionSQLite("ejercicios");

        try {
            ej.ej1();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        ej.cerrarConexion();

    }
}