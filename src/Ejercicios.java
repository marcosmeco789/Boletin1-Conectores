import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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

    public void ej3b(int codAlumno) throws SQLException {
        String query = "DELETE FROM alumnos WHERE codigo = ?";

        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ps.setInt(1, codAlumno);
            int filasAfectadas = ps.executeUpdate();
            System.out.println("Filas eliminadas en asignaturas: " + filasAfectadas);
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
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
        System.out.println("\n----------------\n");

        ResultSet basesDatos = dbmt.getCatalogs();
        while (basesDatos.next()) {
            System.out.println("Nombre BD: " + basesDatos.getString(1));
        }

        System.out.println("\n----------------\n");

        ResultSet rs = dbmt.getTables("add", null, null, null);
        while (rs.next()) {
            System.out.println("Nombre tabla: " + rs.getString("TABLE_NAME"));
            System.out.println("Tipo tabla: " + rs.getString("TABLE_TYPE"));
        }
        System.out.println("\n----------------\n");

        ResultSet rs2 = dbmt.getTables("add", null, null, new String[] { "VIEW" });
        while (rs2.next()) {
            System.out.println("Nombre tabla: " + rs2.getString("TABLE_NAME"));
            System.out.println("Tipo tabla: " + rs2.getString("TABLE_TYPE"));
        }
        System.out.println("\n----------------\n");

        System.out.println("Bases de datos:"); // por orden
        ResultSet db = dbmt.getCatalogs();
        while (db.next()) {
            String nombredb = db.getString(1);
            System.out.println("Nombre BD: " + nombredb);

            ResultSet tablas = dbmt.getTables(nombredb, null, null, null);
            while (tablas.next()) {
                System.out.println("Nombre tabla: " + tablas.getString("TABLE_NAME"));
                System.out.println("Tipo tabla: " + tablas.getString("TABLE_TYPE"));
            }
            System.out.println("-----------------");

        }
        System.out.println("\n----------------\n");

        System.out.println("Procedimientos almacenados:");
        ResultSet procedimientos = dbmt.getProcedures("add", null, null);
        while (procedimientos.next()) {
            System.out.println("Nombre procedimento: " + procedimientos.getString("PROCEDURE_NAME"));
            System.out.println("Tipo procedimiento: " + procedimientos.getString("PROCEDURE_TYPE"));
        }
        System.out.println("\n----------------\n");

        System.out.println("Columnas de las tablas que empiezan por a:");
        ResultSet resu = dbmt.getColumns("add", null, "a%", null);
        while (resu.next()) {
            System.out.println("Posicion columna: " + resu.getString("ORDINAL_POSITION"));
            System.out.println("Base de datos: " + resu.getString("TABLE_CAT"));
            System.out.println("Tabla: " + resu.getString("TABLE_NAME"));
            System.out.println("Nombre columna: " + resu.getString("COLUMN_NAME"));
            System.out.println("Tipo de dato: " + resu.getString("TYPE_NAME"));
            System.out.println("Tamaño: " + resu.getString("COLUMN_SIZE"));
            System.out.println("Permite nulos: " + resu.getString("IS_NULLABLE"));
            System.out.println("Autoincrementado: " + resu.getString("IS_AUTOINCREMENT"));
        }
        System.out.println("\n----------------\n");

        ResultSet todasTablas = dbmt.getTables("add", null, null, null);
        while (todasTablas.next()) {
            String nombreTabla = todasTablas.getString("TABLE_NAME");
            System.out.println("Tabla: " + nombreTabla);

            ResultSet clavesPrimarias = dbmt.getPrimaryKeys("add", null, nombreTabla);
            while (clavesPrimarias.next()) {
                System.out.println("Clave primaria: " + clavesPrimarias.getString("COLUMN_NAME"));
            }

            ResultSet clavesForaneas = dbmt.getExportedKeys("add", null, nombreTabla);
            while (clavesForaneas.next()) {
                System.out.println("Clave foranea: " + clavesForaneas.getString("FKCOLUMN_NAME"));
            }
            System.out.println("\n----------------\n");
        }
    }

    public void ej10() throws SQLException {
        String query = "select *, nombre as non from alumnos";
        try (Statement ps = this.conexion.prepareStatement(query)) {
            ResultSet resu = ps.executeQuery(query);
            ResultSetMetaData rsmd = resu.getMetaData();

            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                System.out.println("Nombre columna: " + rsmd.getColumnName(i));
                System.out.println("Alias columna: " + rsmd.getColumnLabel(i));
                System.out.println("Tipo de dato: " + rsmd.getColumnTypeName(i));
                if (rsmd.isNullable(i) == 0) {
                    System.out.println("Permite nulos: No");
                } else if (rsmd.isNullable(i) == 1) {
                    System.out.println("Permite nulos: Si");
                } else if (rsmd.isNullable(i) == 2) {
                    System.out.println("Permite nulos: Desconocido");
                }
                System.out.println("Autoincrementado: " + rsmd.isAutoIncrement(i));
                System.out.println("\n----------------\n");
            }
        } catch (SQLException e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    public void ej12() throws SQLException {
        try {
            this.conexion.setAutoCommit(false);
            Statement st = this.conexion.createStatement();
            st.executeUpdate("INSERT INTO alumnos (nombre, apellidos, altura, aula) VALUES ('TEST', 'TEST', 150, 20)");
            st.executeUpdate(
                    "INSERT INTO alumnos (nombre, apellidos, altura, aula) VALUES ('TEST2', 'TEST2', 150, 20)");
            st.executeUpdate(
                    "INSERT INTO alumnos (nombre, apellidos, altura, aula) VALUES ('TEST3', 'TEST3', 150, 20)");

            this.conexion.commit();

        } catch (SQLException e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
            try {
                if (this.conexion != null) {
                    System.out.println("Se deshacen los cambios mediante un rollback");

                    this.conexion.rollback();
                }
            } catch (SQLException e1) {
                System.out.println("Error en el rollback: " + e1.getLocalizedMessage());
            }
        }
    }

    public void ej13a(String nombre) throws SQLException {
        String query = "SELECT imagen FROM imagenes WHERE nombre = ?";

        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ps.setString(1, nombre);
            ResultSet resu = ps.executeQuery();
            if (resu.next()) {
                InputStream is = resu.getBinaryStream("imagen");

                java.io.FileOutputStream archivoSalida = new FileOutputStream("imagen.jpg");

                int byteLeido = is.read();
                while (byteLeido != -1) {
                    archivoSalida.write(byteLeido);
                    byteLeido = is.read();
                }
                archivoSalida.close();
                is.close();
            }

        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }

    }


    public void ej13b(String nombre, String imagen) throws SQLException {
        String query = "INSERT INTO imagenes (nombre, imagen) VALUES (?, ?)";


        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ps.setString(1, nombre);
            ps.setBinaryStream(2, new FileInputStream(imagen));

            int result = ps.executeUpdate();
            System.out.println("Filas afectadas: " + result);
            
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }


    public void ej15a(int numero, String patron) throws SQLException {

        String query = "CALL getAulas(?, ?)";

        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
            ps.setInt(1, numero);
            ps.setString(2, patron);
            ResultSet resu = ps.executeQuery();
            while (resu.next()) {
                System.out.printf("%-10s %-20s %-10s%n", resu.getString("numero"), resu.getString("nombreAula"), resu.getString("puestos"));
            }
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }

    }

    public void ej15b() throws SQLException {
        String query = "SELECT SUM(puestos) as suma FROM aulas";

        try (Statement ps = this.conexion.createStatement()) {
            ResultSet resu = ps.executeQuery(query);
            while (resu.next()) {
                System.out.println(resu.getString("suma"));
            }
        } catch (Exception e) {
            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
        }
    }

    // Realiza un método que permita buscar una cadena de texto en cualquier columna de tipo char o varchar de cualquier tabla de una base datos dada. Debe indicar la base de datos, tabla y columna donde se encontró la coincidencia y el texto completo del campo
    public void ej16(String bd, String texto) throws SQLException {

        DatabaseMetaData dbmt = this.conexion.getMetaData();


            ResultSet tablas = dbmt.getTables(bd, null, null, null);

            while (tablas.next()) {
                String nombreTabla = tablas.getString("TABLE_NAME");
                ResultSet columnas = dbmt.getColumns(bd, null, nombreTabla, null);

                while (columnas.next()) {
                    String nombreColumna = columnas.getString("COLUMN_NAME");
                    String tipoDato = columnas.getString("TYPE_NAME");

                    if (tipoDato.equals("CHAR") || tipoDato.equals("VARCHAR")) {
                        String query = "SELECT * FROM " + nombreTabla + " WHERE " + nombreColumna + " LIKE ?";
                        try (PreparedStatement ps = this.conexion.prepareStatement(query)) {
                            ps.setString(1, "%" + texto + "%");
                            ResultSet resu = ps.executeQuery();
                            while (resu.next()) {
                                System.out.println("Base de datos: " + bd);
                                System.out.println("Tabla: " + nombreTabla);
                                System.out.println("Columna: " + nombreColumna);
                                System.out.println("Texto completo: " + resu.getString(nombreColumna));
                                System.out.println("\n----------------\n");
                            }
                        } catch (Exception e) {
                            System.out.println("Se ha producido un error: " + e.getLocalizedMessage());
                        }
                    }
                }
            }
        }
    
            
        

        
    

    public static void main(String[] args) {
        Ejercicios ej = new Ejercicios();

        ej.abrirConexion("add", "localhost", "root", "");
        try {
            // ej.ej1("A%", 1);
            // ej.ej2("Marcos", "Ferreira", 162, 31, "Acceso a Datos");
            // ej.ej3a(2);
            // ej.ej3b(1);

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
            // ej.ej10();
            // ej.ej12();
           // ej.ej13a("mario");
            //ej.ej13b("mario", "C:\\Users\\Marcos\\Documents\\subir.jpg");
            //ej.ej15a(20, "a");
            //ej.ej15b();
            ej.ej16("add","%a%");

        } catch (SQLException e) {

            e.printStackTrace();
        }
        ej.cerrarConexion();
    }

}
