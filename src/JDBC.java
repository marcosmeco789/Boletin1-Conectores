
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class JDBC {
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

    public void cerrarConexion() {
        try {
            this.conexion.close();
        } catch (SQLException e) {
            System.out.println("Error al cerrar la conexión: " + e.getLocalizedMessage());
        }
    }

    public void consultaAlumnos(String bd) {
        abrirConexion("add", "localhost", "root", "");
        try (Statement stmt = this.conexion.createStatement()) {
            String query = "select dorsal, nombre, goles from jugadores_celta";
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {

                System.out.println(rs.getInt(1) + "\t" +
                        rs.getString("dorsal") + "\t" + rs.getString(2));
            }
        } catch (SQLException e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void insertarFila() {

        try (Statement sta = this.conexion.createStatement()) {
            String query = "INSERT INTO jugadores_celta VALUES (77, 'Marcos Gonzalez', 'Portero', 19, 'España', 3, 6, 10, 600)";
            // Se ejecuta la sentencia de inserción mediante executeUpdate
            int filasAfectadas = sta.executeUpdate(query);
            System.out.println("Filas insertadas: " + filasAfectadas);
        } catch (SQLException e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void insertarColumna() {
        try (Statement sta = this.conexion.createStatement()) {
            String query = "ALTER TABLE jugadores_celta ADD COLUMN peso INT";
            // Se ejecuta la sentencia de inserción mediante executeUpdate
            int filasAfectadas = sta.executeUpdate(query);
            System.out.println("Columnas insertadas: " + filasAfectadas);
        } catch (SQLException e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    private PreparedStatement ps = null; // atributo de instancia

    public void consultaAlumnosPS(String patron, int numResultados)
            throws SQLException {
        String query = "select * from aulas where nombreAula like ? limit ?";
        if (this.ps == null)
            this.ps = this.conexion.prepareStatement(query);
        ps.setString(1, patron);
        ps.setInt(2, numResultados);
        ResultSet resu = ps.executeQuery();
        while (resu.next()) {
            System.out.println(resu.getInt(1) + "\t" + resu.getString("nombreAula"));
        }

    }

    public void ejemplo7() {

        try (CallableStatement cs = this.conexion.prepareCall("CALL getAulas(?,?)")) {
            // Se proporcionan valores de entrada al procedimiento
            cs.setInt(1, 10);
            cs.setString(2, "o");
            ResultSet resultado = cs.executeQuery();
            try {
                while (resultado.next()) {
                    System.out.println(resultado.getInt(1) + "\t" +
                            resultado.getString("nombreAula") + "\t" + resultado.getInt("puestos"));
                }
            } catch (SQLException e) {

                e.printStackTrace();
            }
        } catch (SQLException e) {

            e.printStackTrace();
        }

    }

    public void getInfo(String bd) {
        DatabaseMetaData dbmt;
        ResultSet tablas, columnas;
        try {
            dbmt = this.conexion.getMetaData();
            tablas = dbmt.getTables(bd, null, null, null);
            while (tablas.next()) {
                System.out.println(String.format("%s %s",
                        tablas.getString("TABLE_NAME"), tablas.getString("TABLE_TYPE")));
                columnas = dbmt.getColumns(bd, null,
                        tablas.getString("TABLE_NAME"), null);
                while (columnas.next()) {
                    System.out.println(String.format(" %s %s %d %s %s",
                            columnas.getString("COLUMN_NAME"),
                            columnas.getString("TYPE_NAME"),
                            columnas.getInt("COLUMN_SIZE"),
                            columnas.getString("IS_NULLABLE"),
                            columnas.getString("IS_AUTOINCREMENT")));
                }
            }
        } catch (SQLException e) {
            System.out.println("Error obteniendo datos " + e.getLocalizedMessage());
        }
    }

    public void getInfoConsulta(String consulta) throws SQLException {
        Statement st = this.conexion.createStatement();
        ResultSet filas = st.executeQuery(consulta);
        ResultSetMetaData rsmd = filas.getMetaData();
        System.out.println("Num\tNombre\tAlias\tTipoDatos");
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            System.out.println(String.format("%d \t %s \t %s \t %s", i,
                    rsmd.getColumnName(i),
                    rsmd.getColumnLabel(i),
                    rsmd.getColumnTypeName(i)));
        }
    }

    public static void main(String[] args) {
        JDBC jdbc = new JDBC();
        jdbc.abrirConexion("add", "localhost", "root", "");
        // jdbc.consultaAlumnos("furbo");
        // jdbc.insertarColumna();
        // jdbc.ejemplo7();
        //jdbc.getInfo("add");
        try {
            jdbc.getInfoConsulta("SELECT * from alumnos");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        jdbc.cerrarConexion();

    }
}
