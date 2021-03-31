# DB_Transition

DB_Transition Es un archivo que está programado para ejecutar en consola que sirve para actualizar datos y esquemas de una base de datos **SQL SERVER**.

La aplicación es principalmente un archivo **.cs** que puede ser importado a algún otro proyecto para ser adaptado.

El método principal:


```C#
//  Recibe un objeto tipo config que debe tener campos utilizados
//  para la conexión a la base de datos.
public static void TransitionDatabases(Config config);

// Ejemplo de la clase Config con la información requerida para el proceso.
public class Config
{
        public string User { get; set; }
        public string Password { get; set; }
        public string Instance { get; set; }
        public string ServerName { get; set; }
        public string ConString { get; set; }
}
```

Para conectar con una cadena de conexión específica, debe enviar ese campo en la propiedad  **ConString**, si desea autenticar con seguridad integrada de Windows, **User = null | User = ""**.


En la ubicación del compilado, debe existir un archivo llamado **TableList.txt**, el cual contendrá el nombre de las tablas que se van a manipular en ambas bases de datos. 
Ambas tablas deben tener el mismo nombre en las dos tablas, sin importar si el esquema cambia, El programa modificará la tabla y le agregará los campos que la otra tabla no tenga o viceversa para posteriormente agregar la información en el orden especificado en el archivo **TableList.txt** para evitar conflictos de **CONSTRAINTS**.

## Usage

**DatabaseTo**: Base de datos a la que se enviará la información y se va a adaptar el esquema.

**DatabaseFrom**: Base de datos de la que se extrae la información para ser incertada en la nueva base de datos.


1.- Al ejecutar el programa, este le pedirá que ingrese los nombres de las bases de datos que serán procesadas.

2.- Luego de ingresarlo, le preguntará si ejecutará instrucciones del archivo (si existe) **prep.sql**, el cual contendrá un script adicional al proceso si lo quiere ejecutar antes de la ejecución. si no existe este archivo, el programa automáticamente lo omitirá y pasará al proceso principal de transición.

3.- Se ejecutará el proceso principal. se mostrará un registro de las tablas afectadas con un indicador de error (si existe) con su mensaje de error devuelto por **SQL SERVER**


**Registro con éxito:**

![alt text](https://github.com/BinaryMasc/DB_Transition/blob/main/img/ex2.PNG)


**Registro con errores:**

![alt text](https://github.com/BinaryMasc/DB_Transition/blob/main/img/ex3.PNG)


4.- El proceso omitirá los registros con llave primaria duplicada e insertará IDENTITY. Dado el caso de que algunas tablas específicas se debe hacer la inserción, se debe definir eso en el archivo **prep.sql** como una instrucción de borrado de registros.

5.- Dado el caso en el que se requiera hacer un cambio (agregar alguna columna) a la base de datos a la que se va actualizar (**TO**) o en el caso de que se requiera una columna que existe en la base de datos **FROM** y no en la **TO**, se intentará Parsear la base de datos que lo requiera (**FROM** y **TO**), incluyendo inserción de candidatos de llaves primarias y foráneas con su tipo de datos correspondiente.

![alt text](https://github.com/BinaryMasc/DB_Transition/blob/main/img/ex4.PNG)

En caso de devolver error, este mensaje se mostrará en rojo y mostrará el motivo de error por cada tabla Parseada.


- Actualmente el programa no tiene la opción de generar una diferencial respecto a la base de datos **TO** si no existe una columna, borrarla de la base de datos **FROM**, este hace una unión full del esquema, es decir, **UNION ALL** de las columnas, las otras posibilidades de hacer **UNION LEFT** y **UNION RIGHT** actualmente siguen en desarrollo:

![alt text](https://dwgeek.com/wp-content/uploads/2019/12/Snowflake-Set-Operators.jpg)

5.- Al terminar el proceso, mostrará el número de tablas afectadas, el número de errores y el número de registros afectados.

![alt text](https://github.com/BinaryMasc/DB_Transition/blob/main/img/ex5.PNG)

6.- Puede crear un archivo llamado **posp.sql**, el cual cumplirá la misma función que el archivo **prep.sql**, solo que este se ejecutará al terminar el proceso.

7.- Al terminar, si ocurre algún error de conexión o en el archivo **posp.sql**, este lo mostrará en la consola y podrá generar un informe detallado de todo el proceso para exportarlo en un archivo **log.txt** en la ruta del ejecutable.


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
