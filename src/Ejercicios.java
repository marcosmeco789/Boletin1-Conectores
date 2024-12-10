import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Ejercicios {

    private Connection conexion;

    public void abrirConexion(String bd, String servidor, String usuario,
            String password) {
        try {
            String url = String.format("jdbc:mysql://localhost:3306/add?useServerPrepStmts=true", "usuario",
                    "contraseña");
            // Establecemos la conexión con la BD
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

    private PreparedStatement ps = null; // atributo de instancia

    public void ej1(String patron, int numResultados) throws SQLException {
        String query = "select * from alumnos where nombre like ? limit ?";
        if (this.ps == null)
            this.ps = this.conexion.prepareStatement(query);
        ps.setString(1, patron);
        ps.setInt(2, numResultados);
        ResultSet resu = ps.executeQuery();
        while (resu.next()) {
            System.out.println(resu.getInt(1) + "\t" + resu.getString("nombre"));
        }
    }

    public void ej2(String nombre, String apellidos, int altura, int aula, String nombreAsignatura)
            throws SQLException {
        String query1 = "INSERT INTO alumnos (nombre, apellidos, altura, aula) VALUES (?, ?, ?, ?)";
        String query2 = "INSERT INTO asignaturas (NOMBRE) VALUES (?)";

        try (PreparedStatement psAlumnos = this.conexion.prepareStatement(query1);
                PreparedStatement psAsignaturas = this.conexion.prepareStatement(query2)) {

            psAlumnos.setString(1, nombre);
            psAlumnos.setString(2, apellidos);
            psAlumnos.setInt(3, altura);
            psAlumnos.setInt(4, aula);
            int filasAfectadas1 = psAlumnos.executeUpdate();
            System.out.println("Filas insertadas en alumnos: " + filasAfectadas1);

            psAsignaturas.setString(1, nombreAsignatura);
            int filasAfectadas2 = psAsignaturas.executeUpdate();
            System.out.println("Filas insertadas en asignaturas: " + filasAfectadas2);

        } catch (SQLException e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }

    }

    public void ej3a(int codigoAsignatura) throws SQLException {
        String query = "DELETE FROM asignaturas WHERE COD = ?";

        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ps.setInt(1, codigoAsignatura);
            int filasAfectadas = ps.executeUpdate();
            System.out.println("Filas eliminadas en asignaturas: " + filasAfectadas);
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void borrarAlumnosAsignaturas(int codigo, int COD)
            throws SQLException {
        ps = null;
        String query = "DELETE from alumnos where codigo=?";
        if (this.ps == null)
            this.ps = this.conexion.prepareStatement(query);
        ps.setInt(1, codigo);
        int resu = ps.executeUpdate();

        ps = null;
        String query2 = "DELETE from asignaturas where COD=?";
        if (this.ps == null)
            this.ps = this.conexion.prepareStatement(query2);
        ps.setInt(1, COD);
        int resu2 = ps.executeUpdate();
    }

    public void modificarAlumnos(String nombre, String apellidos, int altura, int aula, int codigo)
            throws SQLException {
        String query = "UPDATE alumnos SET nombre = ?, apellidos = ?, altura = ?, aula = ? WHERE codigo = ?";
        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ps.setString(1, nombre);
            ps.setString(2, apellidos);
            ps.setInt(3, altura);
            ps.setInt(4, aula);
            ps.setInt(5, codigo);

            int filasAfectadas = ps.executeUpdate();
            System.out.println("Filas insertadas en alumnos: " + filasAfectadas);
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void modificarAsignaturas(String nombre, int cod)
            throws SQLException {
        String query = "UPDATE asignaturas SET NOMBRE = ? WHERE COD = ?";
        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ps.setString(1, nombre);
            ps.setInt(2, cod);

            int filasAfectadas = ps.executeUpdate();
            System.out.println("Filas insertadas en alumnos: " + filasAfectadas);
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void ej5a() throws SQLException {
        String query = "select nombreAula from aulas where numero IN (select aula from alumnos)";

        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ResultSet resu = ps.executeQuery();
            while (resu.next()) {
                System.out.println(resu.getString("nombreAula"));

            }
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void ej5b() throws SQLException {
        String query = "SELECT alumnos.nombre, notas.NOTA, asignaturas.NOMBRE from notas JOIN alumnos ON notas.alumno=alumnos.codigo JOIN asignaturas ON notas.asignatura=asignaturas.COD";
        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ResultSet resu = ps.executeQuery();
            while (resu.next()) {
                System.out.printf("%-15s %-10s %-10s%n", resu.getString("alumnos.nombre"), resu.getString("notas.NOTA"),
                        resu.getString("asignaturas.NOMBRE"));
            }
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void ej5c() throws SQLException {
        String query = "SELECT asignaturas.NOMBRE from asignaturas WHERE asignaturas.COD NOT IN (SELECT notas.asignatura from notas)";
        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ResultSet resu = ps.executeQuery();

            while (resu.next()) {
                System.out.println(resu.getString("asignaturas.NOMBRE"));
            }
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void ej6a(int altura, String patron) throws SQLException {
        String query = "SELECT alumnos.nombre FROM alumnos WHERE alumnos.altura>=? AND alumnos.nombre LIKE ?";

        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ps.setInt(1, altura);
            ps.setString(2, patron);
            ResultSet resu = ps.executeQuery();

            while (resu.next()) {
                System.out.println(resu.getString("alumnos.nombre"));

            }
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());

        }
    }

    public void ej6b(int altura, String patron) throws SQLException {
        String query = "SELECT alumnos.nombre FROM alumnos WHERE alumnos.altura>=" + altura
                + " AND alumnos.nombre LIKE " + patron;

        try (Statement stmt = this.conexion.createStatement()) {
            ResultSet resu = stmt.executeQuery(query);

            while (resu.next()) {
                System.out.println(resu.getString("alumnos.nombre"));
            }
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void ej7() throws SQLException {

        long msInicialP = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            ej6a(170, "'P%'");
        }
        long msFinalP = System.currentTimeMillis();
        System.out.println(msFinalP - msInicialP + " milisegundos la preparada");
        long msInicialNP = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            ej6b(170, "'P%'");

        }
        long msFinalNP = System.currentTimeMillis();
        System.out.println(msFinalNP - msInicialNP + " milisegundos no preparada");

    }

    public void ej8(String tabla, String nombreCampo, String tipoDato, String propiedades) throws SQLException {
        String query = "ALTER TABLE ? ADD COLUMN ? ? ?";

        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ps.setString(1, tabla);
            ps.setString(2, nombreCampo);
            ps.setString(3, tipoDato);
            ps.setString(4, propiedades);

            int result = ps.executeUpdate();
            System.out.println("Filas acertadas: " + result);
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void ej8t() throws SQLException {
        String query = "ALTER TABLE alumnos ADD COLUMN Prueba2 varchar(30) null";

        try (Statement ps = this.conexion.prepareStatement(query)) {

            int result = ps.executeUpdate(query);
            System.out.println(result);
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void ej9() throws SQLException {
        DatabaseMetaData dbmt = this.conexion.getMetaData();

        System.out.println("Nombre driver: " + dbmt.getDriverName());
        System.out.println("Version driver: " + dbmt.getDriverVersion());
        System.out.println("URL conexion: " + dbmt.getURL());
        System.out.println("Usuario con el que estamos conectados: " + dbmt.getUserName());
        System.out.println("Nombre SGBD: " + dbmt.getDatabaseProductName());
        System.out.println("version SGBD: " + dbmt.getDatabaseProductVersion());
        System.out.println("Palabras reservadas: " + dbmt.getSQLKeywords() + "\n\n");

        System.out.println("Bases de dados del sgdb: " + dbmt.getCatalogs());
    }

    public static void main(String[] args) {
        Ejercicios ej = new Ejercicios();

        ej.abrirConexion("add", "localhost", "root", "");
        try {
            // ej.ej1("A%", 1);
            // ej.ej2("Marcos", "Ferreira", 162, 31, "Acceso a Datos");
           // ej.ej3a(2);
            //ej.borrarAlumnosAsignaturas(1,1);

            // ej.modificarAlumnos("Marcos", "MArcos", 163, 11, 16);
            // ej.modificarAsignaturas("ProgramaciooonM", 1);
            // ej.ej5a();
            // ej.ej5b();
            // ej.ej5c();
            // ej.ej6a(150, "A%");
            // ej.ej7();
            // ej.ej8("alumnos", "TEST2", "varchar(30)", null);
            // ej.ej8t();
            // ej.ej9();

        } catch (SQLException e) {

            e.printStackTrace();
        }
        ej.cerrarConexion();
    }

}
