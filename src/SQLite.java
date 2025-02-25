import java.sql.Statement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SQLite {
    private Connection conexion;
    private Connection conexionSQLite;

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
            String url = String.format(
                    "jdbc:mysql://localhost:3306/add?useServerPrepStmts=true&jdbcCompliantTruncation=false&zeroDateTimeBehavior=convertToNull",
                    "usuario", "contraseña");
        }

    }

    public void abrirConexionEj10(String bd, String servidor, String usuario,
            String password) {
        try {
            String url = String.format(
                    "jdbc:mysql://localhost:3306/add?useServerPrepStmts=true&jdbcCompliantTruncation=false&zeroDateTimeBehavior=convertToNull",
                    "usuario",
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

            this.conexionSQLite = DriverManager.getConnection(url);
            if (this.conexionSQLite != null) {
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

    // Ejercicio 1: Migra las tablas y la vista de la base de datos ADD a SQLite.
    public void ej1() throws SQLException {
        DatabaseMetaData metaData;
        ResultSet tablas;
        PreparedStatement ps = null;
        try {
            metaData = this.conexion.getMetaData();
            tablas = metaData.getTables("add", null, null, null);
            while (tablas.next()) {
                String nombreTabla = tablas.getString("TABLE_NAME");
                String query = "SHOW CREATE TABLE " + nombreTabla;
                ps = this.conexion.prepareStatement(query);

                ResultSet result = ps.executeQuery();
                while (result.next()) {
                    System.out.println(result.getString(2));
                    String createTable = result.getString(2);
                    createTable = createTable.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                    createTable = createTable.replace("KEY `aula` (`aula`),", "");
                    createTable = createTable.replace("AUTO_INCREMENT", "");
                    createTable = createTable.replace("ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci", "");
                    createTable = createTable.replace("ENGINE=InnoDB", "");

                    // Ejecutar la consulta de creación de tabla en SQLite
                    try (Statement stmt = this.conexionSQLite.createStatement()) {
                        stmt.executeUpdate(createTable);
                    } catch (SQLException e) {
                        System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }
    }

    // public void ej3(){
    // SELECT nombreAula, puestos FROM aulas ORDER BY puestos DESC LIMIT 2 OFFSET 1;

    // }

    public void ej4a() throws SQLException {
        String query = "SELECT nombreAula FROM aulas WHERE puestos>=33";

        try (Statement stmt = this.conexionSQLite.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }

        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }
    }

    public void ej4b(int minimo) throws SQLException {
        String query = "SELECT nombreAula FROM aulas WHERE puestos>=?";

        try (PreparedStatement ps = this.conexionSQLite.prepareStatement(query)) {
            ps.setInt(1, minimo);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }

        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }
    }

    public void ej5() throws SQLException {
        String query = "INSERT INTO aulas VALUES ('35', 'Aula Clase', '40')";

        try (Statement stmt = this.conexionSQLite.createStatement()) {
            int afectados = stmt.executeUpdate(query);

            System.out.println("Filas afectadas: " + afectados);

        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }
    }

    public void ej6(int numero, String nombre, int puestos) throws SQLException {
        String query = "REPLACE INTO aulas (numero, nombreAula, puestos) VALUES (?, ?, ?)";

        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ps.setInt(1, numero);
            ps.setString(2, nombre);
            ps.setInt(3, puestos);
            int afectados = ps.executeUpdate();

            System.out.println("Filas afectadas: " + afectados);
        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }
    }

    public void ej7(String nombre, String apellidos, int altura, int aula) throws SQLException {
        String queryMySQL = "INSERT INTO alumnos (nombre, apellidos, altura, aula) VALUES (?, ?, ?, ?)";
        String querySQLite = "INSERT INTO alumnos (nombre, apellidos, altura, aula) VALUES (?, ?, ?, ?)";

        try (PreparedStatement psMySQL = this.conexion.prepareStatement(queryMySQL);
                PreparedStatement psSQLite = this.conexionSQLite.prepareStatement(querySQLite)) {

            psMySQL.setString(1, nombre);
            psMySQL.setString(2, apellidos);
            psMySQL.setInt(3, altura);
            psMySQL.setInt(4, aula);

            psSQLite.setString(1, nombre);
            psSQLite.setString(2, apellidos);
            psSQLite.setInt(3, altura);
            psSQLite.setInt(4, aula);

            int afectadosMySQL = psMySQL.executeUpdate();
            int afectadosSQLite = psSQLite.executeUpdate();

            System.out.println("Filas afectadas en MySQL: " + afectadosMySQL);
            System.out.println("Filas afectadas en SQLite: " + afectadosSQLite);
        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }
    }

    public void ej8(String nombreAula) throws SQLException {

        String queryMySQL = "SELECT nombreAula FROM aulas WHERE nombreAula LIKE ?";
        String querySQLite = "SELECT nombreAula FROM aulas WHERE nombreAula LIKE ?";

        try (PreparedStatement psMySQL = this.conexion.prepareStatement(queryMySQL);
                PreparedStatement psSQLite = this.conexionSQLite.prepareStatement(querySQLite)) {

            psMySQL.setString(1, nombreAula);
            psSQLite.setString(1, nombreAula);

            ResultSet rsMySQL = psMySQL.executeQuery();
            ResultSet rsSQLite = psSQLite.executeQuery();

            System.out.println("Resultados en MySQL:");
            while (rsMySQL.next()) {
                System.out.println(rsMySQL.getString(1));
            }

            System.out.println("Resultados en SQLite:");
            while (rsSQLite.next()) {
                System.out.println(rsSQLite.getString(1));
            }

        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }

    }

    public void ej9(String nombre, String apellidos, int altura, int aula) throws SQLException {
        String queryMySQL = "INSERT INTO alumnos (nombre, apellidos, altura, aula) VALUES (?, ?, ?, ?)";
        String querySQLite = "INSERT INTO alumnos (nombre, apellidos, altura, aula) VALUES (?, ?, ?, ?)";

        try (PreparedStatement psMySQL = this.conexion.prepareStatement(queryMySQL);
                PreparedStatement psSQLite = this.conexionSQLite.prepareStatement(querySQLite)) {

            psMySQL.setString(1, nombre);
            psMySQL.setString(2, apellidos);
            psMySQL.setInt(3, altura);
            psMySQL.setInt(4, aula);

            psSQLite.setString(1, nombre);
            psSQLite.setString(2, apellidos);
            psSQLite.setInt(3, altura);
            psSQLite.setInt(4, aula);

            this.conexion.setAutoCommit(false);
            this.conexionSQLite.setAutoCommit(false);

            int afectadosMySQL = psMySQL.executeUpdate();
            int afectadosSQLite = psSQLite.executeUpdate();

            if (afectadosMySQL > 0 && afectadosSQLite > 0) {
                this.conexion.commit();
                this.conexionSQLite.commit();
                System.out.println("Filas afectadas en MySQL: " + afectadosMySQL);
                System.out.println("Filas afectadas en SQLite: " + afectadosSQLite);
                System.out.println("Se han insertado los datos correctamente");
            } else {
                this.conexion.rollback();
                this.conexionSQLite.rollback();
                System.out.println("No se han podido insertar los datos");
            }

        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }
    }

    public void ej10ab(String nombre, String fecha) throws SQLException {
        String queryMySQL = "INSERT INTO fechas (nombre, fecha) VALUES (?, ?)";
        //String querySQLite = "INSERT INTO fechas (nombre, fecha) VALUES (?, ?)";

        try (PreparedStatement psMySQL = this.conexion.prepareStatement(queryMySQL)
                /*PreparedStatement psSQLite = this.conexionSQLite.prepareStatement(querySQLite)*/) {

            psMySQL.setString(1, nombre);
            psMySQL.setString(2, fecha);

            //psSQLite.setString(1, nombre);
            //psSQLite.setString(2, fecha);

            int afectadosMySQL = psMySQL.executeUpdate();
            //int afectadosSQLite = psSQLite.executeUpdate();

            System.out.println("Filas afectadas en MySQL: " + afectadosMySQL);
           // System.out.println("Filas afectadas en SQLite: " + afectadosSQLite);
        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }
    }

    public void ej10c() throws SQLException {
        String queryMySQL = "INSERT INTO fechas (nombre, fecha) VALUES ('ejercicio', '2005-10-05 14:32:00')";
        String querySQLite = "INSERT INTO fechas (nombre, fecha) VALUES ('ejercicio', '2003-10-05 14:32:00')";

        try (Statement stmtMySQL = this.conexion.createStatement();
                Statement stmtSQLite = this.conexionSQLite.createStatement()) {

            int afectadosMySQL = stmtMySQL.executeUpdate(queryMySQL);
            int afectadosSQLite = stmtSQLite.executeUpdate(querySQLite);

            System.out.println("Filas afectadas en MySQL: " + afectadosMySQL);
            System.out.println("Filas afectadas en SQLite: " + afectadosSQLite);
        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }
    }

    public void ej10d() throws SQLException {
        String queryMySQL = "INSERT INTO fechas (nombre, fecha) VALUES ('actual', NOW())";
        String querySQLite = "INSERT INTO fechas (nombre, fecha) VALUES ('actual', datetime('now'))";

        try (Statement stmtMySQL = this.conexion.createStatement();
                Statement stmtSQLite = this.conexionSQLite.createStatement()) {

            int afectadosMySQL = stmtMySQL.executeUpdate(queryMySQL);
            int afectadosSQLite = stmtSQLite.executeUpdate(querySQLite);

            System.out.println("Filas afectadas en MySQL: " + afectadosMySQL);
            System.out.println("Filas afectadas en SQLite: " + afectadosSQLite);
        } catch (SQLException e) {
            System.out.println("Error al ejecutar la consulta: " + e.getLocalizedMessage());
        }
    }

    public static void main(String[] args) {
        SQLite ej = new SQLite();

        // ej.abrirConexion("add", "localhost", "root", "");
         ej.abrirConexionEj10("add", "localhost", "root", "");
        //ej.abrirConexionSQLite("ejercicios");

        try {
            ej.ej10ab("Fecha2 2", "");
            // ej.ej10c();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        ej.cerrarConexion();

    }
}