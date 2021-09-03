using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PerseToolkit
{
    public partial class TransitionDB
    {
		private static string ReportProcessTable(bool error, string message, int rowsAffected, int rowsFound)
		{
			string log = "";

			if (error) Console.ForegroundColor = ConsoleColor.Red;
			else Console.ForegroundColor = ConsoleColor.Green;

			Console.WriteLine("Error: " + error.ToString());
			log += "\nError: " + error.ToString();

			Console.ResetColor();

			Console.Write($"{(error ? "Message: " + message + "\n" : "")}");
			log += $"{(error ? "\nMessage: " + message + "\n" : "")}";

			Console.WriteLine("Rows Affected: " + rowsAffected + "/" + rowsFound);
			log += "\nRows Affected: " + rowsAffected + "/" + rowsFound;

			return log;
		}

		/// <summary>
		///     Realiza una consulta a la tabla especificada de la base de datos de a la que se enviará la información (desde la perspectiva de la BBDD master) y devuelve el nombre de las columnas
		/// </summary>
		/// <returns>El nombre de las columnas separados por coma</returns>
		public static string GetColumnNames(SqlConnection CON, string tableName)
		{
			string ret = " ";

			using (SqlDataReader data = new SqlCommand($"SELECT TOP 1 * FROM {tableName}", CON).ExecuteReader())
			{
				data.Read();

				for (int i = 0; i < data.FieldCount; i++)
				{
					if (i == 0) ret += "  " + '[' + data.GetName(i) + ']';
					else ret += ",  " + '[' + data.GetName(i) + ']';
				}
			}
			return ret;
		}

		/// <summary>
		///     Realiza una consulta a la tabla especificada de la base de datos de a la que se enviará la información (desde la perspectiva de la BBDD master) y devuelve el nombre de las columnas
		/// </summary>
		/// <returns>Devuelve un array del nombre de las columnas</returns>
		public static string[] GetArrayColumnNames(SqlConnection CON, string tableName)
		{
			List<string> ret = new List<string>();

			using (SqlDataReader data = new SqlCommand($"SELECT TOP 1 * FROM {tableName}", CON).ExecuteReader())
			{
				data.Read();

				for (int i = 0; i < data.FieldCount; i++)
				{
					ret.Add(data.GetName(i));
				}
			}
			return ret.ToArray();
		}


		/// <summary>
		///     Compara mediante dos arrays con los nombres de la tabla relacionada si contiene columnas de más
		/// </summary>
		/// <returns>Devuelve en parámetros por referencias, las listas (string) de las columnas por base de datos</returns>
		private static void GetDiffTables(out List<string> ColumnsThatDBFromNtHave,
										  out List<string> ColumnsThatDBToNtHave,
										  string[] columnsDBFrom,
										  string[] columnsDBTo)
		{

			ColumnsThatDBFromNtHave = new List<string>();
			ColumnsThatDBToNtHave = new List<string>();


			int j;
			int k;


			//	Get table Names that were not found in DBTo
			for (j = 0; j < columnsDBFrom.Length; j++)
			{
				bool foundColumn = false;

				for (k = 0; k < columnsDBTo.Length; k++)
				{
					if (columnsDBFrom[j].Equals(columnsDBTo[k]))
					{
						foundColumn = true;
						break;
					}
				}
				if (!foundColumn)
				{
					ColumnsThatDBToNtHave.Add(columnsDBFrom[j]);
				}
			}

			//	Get table Names that were not found in DBFrom
			for (j = 0; j < columnsDBTo.Length; j++)
			{
				bool foundColumn = false;

				for (k = 0; k < columnsDBFrom.Length; k++)
				{
					if (columnsDBTo[j].Equals(columnsDBFrom[k]))
					{
						foundColumn = true;
						break;
					}
				}
				if (!foundColumn)
				{
					ColumnsThatDBFromNtHave.Add(columnsDBTo[j]);
				}
			}
		}

		/// <summary>
		///     Realiza una consulta a la tabla especificada de la base de datos de a la que se enviará la información y devuelve el nombre de las llaves primarias (PK)
		/// </summary>
		/// <returns>El nombre de las llaves primarias separados por coma</returns>
		private static string GetPK(SqlConnection con, string tableName, string databaseName)
		{
			//  Query for get PK List
			string Query = $"use {databaseName}; " +
				"SELECT KU.table_name as TABLENAME " +
				",column_name as PRIMARYKEYCOLUMN " +
				"FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC " +
				"INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU " +
				"ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
				"AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME " +
				$"AND KU.table_name='{tableName}' " +
				"ORDER BY " +
				"KU.TABLE_NAME " +
				",KU.ORDINAL_POSITION; " +
				"use master;"; //   Back to use the master DB

			string ret = "";

			using (SqlDataReader data = new SqlCommand(Query, con).ExecuteReader())
			{
				int i = 0;

				while (data.Read())
				{
					if (i == 0 && !string.IsNullOrEmpty(data[1].ToString()))
						ret = data[1].ToString();
					else ret += ", " + data[1].ToString();

					i++;
				}
			}

			return ret;


		}

		/// <summary>
		///     Realiza una consulta a la tabla especificada de la base de datos de a la que se enviará la información y devuelve el nombre de las llaves primarias (PK)
		/// </summary>
		/// <returns>Un array con los nombres de las llaves primarias de la tabla</returns>
		private static string[] GetArrayPK(SqlConnection con, string tableName, string databaseName)
		{
			//  Query for get PK List
			string Query = $"use {databaseName}; " +
				"SELECT KU.table_name as TABLENAME " +
				",column_name as PRIMARYKEYCOLUMN " +
				"FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC " +
				"INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU " +
				"ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
				"AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME " +
				$"AND KU.table_name='{tableName}' " +
				"ORDER BY " +
				"KU.TABLE_NAME " +
				",KU.ORDINAL_POSITION; " +
				"use master;"; //   Back to use the master DB

			List<string> ret = new List<string>();

			using (SqlDataReader data = new SqlCommand(Query, con).ExecuteReader())
			{

				while (data.Read())
				{
					ret.Add(data[1].ToString());
				}
			}

			return ret.ToArray();


		}

		/// <summary>
		///     Realiza una consulta a la tabla especificada de la base de datos de a la que se enviará la información y devuelve el nombre de las llaves foráneas (FK)
		/// </summary>
		/// <returns>Un array con los nombres de las llaves foráneas de la tabla</returns>
		private static string[] GetArrayFK(SqlConnection con, string tableName, string databaseName)
		{
			//  Query for get PK List
			string Query = $"use {databaseName}; " +
				"SELECT KU.table_name as TABLENAME " +
				",column_name as PRIMARYKEYCOLUMN " +
				"FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC " +
				"INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU " +
				"ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
				"AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME " +
				$"AND KU.table_name='{tableName}' " +
				"ORDER BY " +
				"KU.TABLE_NAME " +
				",KU.ORDINAL_POSITION; " +
				"use master;"; //   Back to use the master DB

			List<string> ret = new List<string>();

			using (SqlDataReader data = new SqlCommand(Query, con).ExecuteReader())
			{

				while (data.Read())
				{
					ret.Add(data[1].ToString());
				}
			}

			return ret.ToArray();


		}

		/// <summary>
		///     
		/// </summary>
		/// <returns></returns>
		public static TableInfo GetTableInfo(SqlConnection con, string tableName, string databaseName)
		{
			char jl = '\n';

			//	CMD for get column info
			string cmd_get = $"use [{databaseName}];" + jl +
							 "SELECT C.COLUMN_NAME AS Name, " + jl +
							 "C.DATA_TYPE AS DataType, " + jl +
							 "C.CHARACTER_MAXIMUM_LENGTH AS Size, " + jl +
							 "C.IS_NULLABLE AS AllowNulls, " + jl +
							 "C.COLUMN_DEFAULT AS DefaultValue, " + jl +
							 "U.CONSTRAINT_NAME AS PrimaryKey " + jl +
							 "FROM INFORMATION_SCHEMA.COLUMNS C FULL OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE U ON C.COLUMN_NAME = U.COLUMN_NAME" + jl +
							 $"WHERE C.TABLE_NAME = '{tableName}' ";// AND U.TABLE_NAME = '{tableName}';";

			SqlDataReader reader = new SqlCommand(cmd_get, con).ExecuteReader();

			DataRows data = new DataRows(reader, false);

			reader.Close();

			//	PrimaryKeys
			cmd_get += $" and U.TABLE_NAME = '{tableName}';";

			reader = new SqlCommand(cmd_get, con).ExecuteReader();

			DataRows PrimaryKeys = new DataRows(reader, false);

			reader.Close();



			//	Set info (Name, dataType, Size, AllowNulls, DefaultValue, PrimaryKeys)

			List<ColumnInfo> tableSchema = new List<ColumnInfo>();

			for (int i = 0; i < data.RowCount; i++)
			{

				string name = data.FindData("Name", i);
				bool isPrimaryKey = false;
				string namePrimaryKey = "";
				string type = data.FindData("DataType", i) + (string.IsNullOrEmpty(data.FindData("Size", i)) ||
							  data.FindData("DataType", i).Contains("ntext") ? "" : $"({data.FindData("Size", i)})"); // if (columnsRemaining[i].Type.Contains("ntext")) columnsRemaining[i].Type = "ntext";


				if (i.Equals(0) || !name.Equals(data.FindData("Name", i - 1)))
				{

					for (int j = 0; j < PrimaryKeys.RowCount; j++)
					{
						if (data.FindData("Name", i).Equals(PrimaryKeys.FindData("Name", j)) && PrimaryKeys.FindData("PrimaryKey", j).Contains("PK"))
						{
							namePrimaryKey = PrimaryKeys.FindData("PrimaryKey", j);
							isPrimaryKey = true;
						}

					}

					tableSchema.Add(new ColumnInfo
					{
						Name = name,
						AllowNullValues = data.FindData("AllowNulls", i).Equals("YES"),
						DefaultValue = data.FindData("DefaultValue", i),
						Type = type,
						IsPrimaryKey = isPrimaryKey,
						PrimaryKeys = isPrimaryKey ? new List<string>() : null
					});

					if (isPrimaryKey)
					{
						tableSchema.Last().PrimaryKeys.Add(namePrimaryKey);
						continue;
					}
				}
				else if (name.Equals(data.FindData("Name", i - 1)))
				{
					for (int j = 0; j < PrimaryKeys.RowCount; j++)
					{
						if (data.FindData("Name", i).Equals(PrimaryKeys.FindData("Name", j)) && PrimaryKeys.FindData("PrimaryKey", j).Contains("PK"))
						{
							namePrimaryKey = PrimaryKeys.FindData("PrimaryKey", j);
							isPrimaryKey = true;
						}

					}

					if (isPrimaryKey)
					{
						if (tableSchema.Last().PrimaryKeys.Equals(null)) tableSchema[tableSchema.Count - 1].PrimaryKeys = new List<string>();

						tableSchema.Last().PrimaryKeys.Add(namePrimaryKey);
					}
				}

			}


			//	Get FKs
			cmd_get = "SELECT  obj.name AS FK_NAME," + jl +
				"tab1.name AS [TableName]," + jl +
				"col1.name AS [column]," + jl +
				"tab2.name AS [referenced_table]," + jl +
				"col2.name AS [referenced_column]" + jl +
				"FROM sys.foreign_key_columns fkc" + jl +
				"INNER JOIN sys.objects obj" + jl +
				"ON obj.object_id = fkc.constraint_object_id" + jl +
				"INNER JOIN sys.tables tab1" + jl +
				"ON tab1.object_id = fkc.parent_object_id" + jl +
				"INNER JOIN sys.schemas sch" + jl +
				"ON tab1.schema_id = sch.schema_id" + jl +
				"INNER JOIN sys.columns col1" + jl +
				"ON col1.column_id = parent_column_id AND col1.object_id = tab1.object_id" + jl +
				"INNER JOIN sys.tables tab2" + jl +
				"ON tab2.object_id = fkc.referenced_object_id" + jl +
				"INNER JOIN sys.columns col2" + jl +
				"ON col2.column_id = referenced_column_id AND col2.object_id = tab2.object_id" + jl +
				$"where tab1.name = '{tableName}';" + jl +
				"use master;";

			reader = new SqlCommand(cmd_get, con).ExecuteReader();

			data = new DataRows(reader, false);

			reader.Close();

			List<ForeignKeyInfo> FKeys = new List<ForeignKeyInfo>();

			for (int i = 0; i < data.RowCount; i++)
			{
				FKeys.Add(new ForeignKeyInfo
				{
					FK_Name = data.FindData("FK_NAME", i),
					TableName = data.FindData("TableName", i),
					ColumnName = data.FindData("column", i),
					ReferencedTable = data.FindData("referenced_table", i),
					ReferencedColumn = data.FindData("referenced_column", i)
				});
			}

			//cmd_get = $"select columnproperty(object_id('{tableName}'),'{}','IsIdentity')";

			for(int i = 0; i < tableSchema.Count; i++)
            {
				cmd_get = $"use {databaseName};\n " +
					$"select columnproperty(object_id('{tableName}'),'{tableSchema[i].Name}','IsIdentity') AS isIdentity;" +
					$"use master;\n ";

				reader = new SqlCommand(cmd_get, con).ExecuteReader();
				data = new DataRows(reader,false);
				reader.Close();

				string id = data.FindData("isIdentity");

				if (string.IsNullOrEmpty(id) || id == "0") continue;

				else
                {
					tableSchema[i].IsIdentity = true;
				}

			}



			return new TableInfo
			{
				Columns = tableSchema,
				Foreign_Keys = FKeys.ToArray(),
				Name = tableName
			};
		}

		/// <summary>
		///		Get all columns from a table of a database
		/// </summary>
		/// <returns></returns>
		private static string[] GetAllTables(SqlConnection con, string databaseName)
        {
			string query = @"use " + databaseName + @";
SELECT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG='" + databaseName.Replace("[", "").Replace("]", "") + "'";

			SqlDataReader reader = new SqlCommand(query, con).ExecuteReader();

			DataRows data = new DataRows(reader, false);

			reader.Close();

			List<string> list = new List<string>();

			for (int i = 0; i < data.RowCount; i++)
				list.Add(data.FindData("TABLE_NAME", i));

			return list.ToArray();

		}


		//--Subprocesses

		/// <summary>
		///     
		/// </summary>
		/// <returns></returns>
		private static bool GenerateSchemaDiff(List<string> ColumnsThatDBFromNtHave,
											  //List<string> ColumnsThatDBToNtHave,
											  string tablename,
											  string DatabaseFrom,
											  string DatabaseTo,
											  SqlConnection con,
											  out string errorGenerateSchema,
											  bool isDBTo)
		{
			errorGenerateSchema = "";

			string CMD_AlterTable = "";

			int n_columnsFromRemain = ColumnsThatDBFromNtHave.Count;
			//int n_columnsToRemain = ColumnsThatDBToNtHave.Count;

			if (n_columnsFromRemain > 0)
			{

				//	Get specific info of the diff
				List<ColumnInfo> columnsRemaining = new List<ColumnInfo>();

				TableInfo tableTo = GetTableInfo(con, tablename, DatabaseTo);

				int columnsCount_To = tableTo.Columns.Count;
				//int foreignKeysCount_To = tableTo.Foreign_Keys.Length;


				for (int i = 0; i < n_columnsFromRemain; i++)
				{
					for (int j = 0; j < columnsCount_To; j++)
					{
						if (ColumnsThatDBFromNtHave[i].Equals(tableTo.Columns[j].Name))
						{
							columnsRemaining.Add(tableTo.Columns[j]);
							break;
						}
					}
				}

				//	Add the fields remainings and PrimaryKeys
				CMD_AlterTable = $"use [{DatabaseFrom}];\n";

				bool hasPrimaryKeys = false;

				//for (int i = 0; i < n_columnsFromRemain; i++)
				for (int i = 0; i < columnsRemaining.Count; i++)
				{

					//	Add column
					if (!columnsRemaining[i].IsForeignKey)
						CMD_AlterTable += $"ALTER TABLE {tablename} " +
										  $"ADD {columnsRemaining[i].Name} {columnsRemaining[i].Type} {(columnsRemaining[i].AllowNullValues ? "NULL" : "NOT NULL")} {(!string.IsNullOrEmpty(columnsRemaining[i].DefaultValue) ? $"DEFAULT {columnsRemaining[i].DefaultValue}" : "")}" +
										  //	In case of NOT NULL is enabled and Not has DEFAULT Value
										  $"{(!columnsRemaining[i].AllowNullValues && string.IsNullOrEmpty(columnsRemaining[i].DefaultValue) ? " DEFAULT '0'" : "")};\n";

					if (columnsRemaining[i].IsPrimaryKey) hasPrimaryKeys = true;
				}


				for (int i = 0; i < columnsCount_To && hasPrimaryKeys && isDBTo; i++)
				{
					//	Add primary Key
					if (columnsRemaining[i].IsPrimaryKey)
						CMD_AlterTable += $"ALTER TABLE {tablename} ADD PRIMARY KEY ({columnsRemaining[i].Name});\n";
				}

				CMD_AlterTable += "\n";


				//	Matrix of ForeignKeyInfo
				List<List<ForeignKeyInfo>> foreign_KeyRemaining = new List<List<ForeignKeyInfo>>();

				//	Add Foreign Keys
				for (int i = 0; i < tableTo.Foreign_Keys.Length; i++)
				{
					//bool addKey = false;

					for (int j = 0; j < columnsRemaining.Count; j++)
					{
						if (columnsRemaining[j].Name == tableTo.Foreign_Keys[i].ColumnName)
						{
							bool exist_FK = false;
							int k;
							for (k = 0; k < foreign_KeyRemaining.Count; k++)
							{
								if (foreign_KeyRemaining[k][0].ColumnName == columnsRemaining[j].Name)
								{
									exist_FK = true;
									break;
								}
							}

							if (!exist_FK)
							{
								foreign_KeyRemaining.Add(new List<ForeignKeyInfo>());
								foreign_KeyRemaining.Last().Add(tableTo.Foreign_Keys[i]);
							}
							else
							{
								foreign_KeyRemaining[k].Add(tableTo.Foreign_Keys[i]);

							}
						}
					}

					/*
					for(int j = 0; j < columnsRemaining.Count; j++)
						if (tableTo.Foreign_Keys[i].ColumnName.Equals(columnsRemaining[j].Name)) addKey = true;

                    if (addKey)
                    {
                        if (first_it)
                        {
							first_it = false;
							keyAdded = true;
							CMD_AlterTable += $"\nALTER TABLE {tablename} DROP CONSTRAINT {tableTo.Foreign_Keys[i].FK_Name};\n";
							CMD_AlterTable += $"\nALTER TABLE {tablename} ADD CONSTRAINT {tableTo.Foreign_Keys[i].FK_Name} FOREIGN KEY ({tableTo.Foreign_Keys[i].})" +
											  $"REFERENCES ({tableTo.Foreign_Keys[i].ColumnName}";
							continue;
						}
                        else
                        {
							CMD_AlterTable += $", {tableTo.Foreign_Keys[i].ColumnName}";
                        }
                    }*/


				}

				if (foreign_KeyRemaining.Count > 0 && isDBTo)
				{
					for (int i = 0; i < foreign_KeyRemaining.Count; i++)
					{
						CMD_AlterTable += $"\nALTER TABLE {tablename} DROP CONSTRAINT {foreign_KeyRemaining[i][0].FK_Name};\n";
						CMD_AlterTable += $"\nALTER TABLE {tablename} ADD CONSTRAINT {foreign_KeyRemaining[i][0].FK_Name} FOREIGN KEY (";

						for (int j = 0; j < foreign_KeyRemaining[i].Count; j++)
						{
							if (j.Equals(0)) CMD_AlterTable += $"{tableTo.Foreign_Keys[i].ColumnName}";
							else CMD_AlterTable += $", {tableTo.Foreign_Keys[i].ColumnName}";
						}

						CMD_AlterTable += $") References {foreign_KeyRemaining[i][0].ReferencedTable}(";

						for (int j = 0; j < foreign_KeyRemaining[i].Count; j++)
						{
							if (j.Equals(0)) CMD_AlterTable += $"{tableTo.Foreign_Keys[i].ColumnName}";
							else CMD_AlterTable += $", {tableTo.Foreign_Keys[i].ColumnName}";
						}
					}
					CMD_AlterTable += ");\n";
				}


				CMD_AlterTable += "use master;\n";


				try
				{
					int aff = new SqlCommand(CMD_AlterTable, con).ExecuteNonQuery();
				}
				catch (Exception ex)
				{
					errorGenerateSchema = ex.Message;
					return true;
				}



			}


			return false;
		}

		/// <summary>
		/// Genera un string de código Transaq-sql cuya función es crear una tabla con el esquema de la tabla referenciada en los parámetros
		/// </summary>
		/// <param name="con">Conexión a la base de datos abierta y válida</param>
		/// <param name="tableName">Nombre de la tabla de la cual se va a obtener el esquema</param>
		/// <param name="databaseName">Nombre de la base de datos</param>
		/// <returns>Sentencia Transaq-SQL para crear una tabla</returns>
		private static string GenerateFullSchemaTable(SqlConnection con, 
													 string tableName, 
													 string databaseName, 
													 bool includePrimaryKeys = false, 
													 bool includeForeignKeys = false)
        {
			//  Obtener información de la tabla (llaves foráneas especialmente y las columnas)
			TableInfo tableInfo = TransitionDB.GetTableInfo(con, tableName, databaseName);

			string strQuery = "use " + databaseName + ";\n\n";//$"CREATE TABLE {tableName}99 (";



			if (includePrimaryKeys) throw new NotImplementedException("Aún no se desarrolla");

            strQuery += $"CREATE TABLE {tableName} (";


            //  Create the table from Transaq-SQL
            //      Create columns
            int i = 0;
            bool hasPrimaryKeys = false;
            foreach (var columnT in tableInfo.Columns)
            {
                bool last = (tableInfo.Columns.Count - 1) == i;

                strQuery += "\n\t[" + columnT.Name + "] " + columnT.Type +
                        $" {(columnT.AllowNullValues ? "NULL" : "NOT NULL")} {(!string.IsNullOrEmpty(columnT.DefaultValue) && !columnT.IsIdentity ? $"DEFAULT {columnT.DefaultValue}" : "")}" +
                                //	In case of NOT NULL is enabled and Not has DEFAULT Value
                                $"{(!columnT.AllowNullValues && string.IsNullOrEmpty(columnT.DefaultValue) && !columnT.IsIdentity ? " DEFAULT '0'" : "")} {(columnT.IsIdentity ? "IDENTITY(1,1)" : "")} {(last ? "" : ",")}";

                if (columnT.IsPrimaryKey) hasPrimaryKeys = true;

                i++;
            }
            strQuery += "\n);\n";

			strQuery += hasPrimaryKeys ? "--\t(!) Llave primaria detectada, pero no incluída en el query.\n" : "";

            string columns = TransitionDB.GetColumnNames(con, databaseName + ".dbo." + tableName);



			//strQuery += $"\nALTER TABLE {tableName} DROP CONSTRAINT {foreign_Keys[i][0].FK_Name};\n";"

			if (!includeForeignKeys && !includePrimaryKeys) return strQuery;


            //	    Matrix of ForeignKeyInfo
            List<List<ForeignKeyInfo>> foreign_Keys = new List<List<ForeignKeyInfo>>();

            //	    Add Foreign Keys
            for (i = 0; i < tableInfo.Foreign_Keys.Length; i++)
            {
                for (int j = 0; j < tableInfo.Columns.Count; j++)
                {
                    if (tableInfo.Columns[j].Name == tableInfo.Foreign_Keys[i].ColumnName)
                    {
                        bool exist_FK = false;
                        int k;
                        for (k = 0; k < foreign_Keys.Count; k++)
                        {
                            if (foreign_Keys[k][0].ColumnName == tableInfo.Columns[j].Name)
                            {
                                exist_FK = true;
                                break;
                            }
                        }

                        if (!exist_FK)
                        {
                            foreign_Keys.Add(new List<ForeignKeyInfo>());
                            foreign_Keys.Last().Add(tableInfo.Foreign_Keys[i]);
                        }
                        else
                        {
                            foreign_Keys[k].Add(tableInfo.Foreign_Keys[i]);

                        }
                    }
                }
            }

            string strQuery2 = "--	Llaves\n";
            if (foreign_Keys.Count > 0)
            {
                for (i = 0; i < foreign_Keys.Count; i++)
                {
                    //strQuery += $"\nALTER TABLE {tableName} DROP CONSTRAINT {foreign_Keys[i][0].FK_Name};\n";
                    strQuery2 += $"\nALTER TABLE {tableName} ADD CONSTRAINT {foreign_Keys[i][0].FK_Name} FOREIGN KEY (";

                    for (int j = 0; j < foreign_Keys[i].Count; j++)
                    {
                        if (j.Equals(0)) strQuery2 += $"{tableInfo.Foreign_Keys[i].ColumnName}";
                        else strQuery2 += $", {tableInfo.Foreign_Keys[i].ColumnName}";
                    }

                    strQuery2 += $") References {foreign_Keys[i][0].ReferencedTable}(";

                    for (int j = 0; j < foreign_Keys[i].Count; j++)
                    {
                        if (j.Equals(0)) strQuery2 += $"{tableInfo.Foreign_Keys[i].ColumnName}";
                        else strQuery2 += $", {tableInfo.Foreign_Keys[i].ColumnName}";
                    }

                    strQuery2 += ");\n";
                }
            }
            strQuery2 += "\n";

            DataRows dr = new DataRows(new SqlCommand($"USE {databaseName};\nselect * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where TABLE_NAME = '{tableName}'", con).ExecuteReader(), true);


			/*
            //      Create primary Keys
            for (i = 0; i < tableInfo.Columns.Count && hasPrimaryKeys; i++)
            {
                if (tableInfo.Columns[i].IsPrimaryKey)
                    //strQuery += $"ALTER TABLE {tableName} ADD PRIMARY KEY ({tableInfo.Columns[i].Name});\n";
                    strQuery += $"ALTER TABLE {tableName} ADD CONSTRAINT PK_Person PRIMARY KEY(ID, LastName); ";
            }*/

			//

			return strQuery + strQuery2;
        }

		public static void GenerateSchemaMain(Config config)
        {
			string database;
			string table;


			string cmdcon = "";


			SqlConnection con = null;

			//  If i want use integrated security, send User = null 

			if (String.IsNullOrEmpty(config.ConString)) cmdcon = $"Server={config.ServerName + (string.IsNullOrEmpty(config.Instance) ? "" : $"\\{config.Instance}")};Database=master;{(string.IsNullOrEmpty(config.User) ? "Trusted_Connection=True;" : $"User Id={config.User};Password={config.Password};")};";
			else cmdcon = config.ConString;


			Console.WriteLine("Database Name:");
			database = Console.ReadLine();


			Console.WriteLine("Table Name:");
			table = Console.ReadLine();

            try
            {
				con = new SqlConnection(cmdcon);
				con.Open();
			}
			catch(Exception ex)
            {
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine("(!) Se ha producido un error al momento de conectarse a la base de datos: \n" + ex.Message);
				Console.ResetColor();
            }

            try
            {
				Console.WriteLine(GenerateFullSchemaTable(con, table, database, false, true));
			}
			catch(Exception ex)
            {
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine("(!) Se ha producido un error al momento de Consultar info de la tabla: \n" + ex.Message);
				Console.ResetColor();
			}
			Console.WriteLine("\n- Presione cualquier tecla para continuar.");
			Console.ReadKey();
			Console.Clear();
		}
	}
}
