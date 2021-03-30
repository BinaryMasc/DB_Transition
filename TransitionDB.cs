using System;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;
using System.IO;

namespace PerseToolkit
{
	public class TransitionDB
	{
		[STAThread]
		public static void TransitionDatabases(Config config)
		{

			#region Initializations

			string[] tableList = new string[0];

			string database_FROM = "BPerseDATOS";
			string database_TO = "BPerse";


			Console.WriteLine("Name Database From: ");
			database_FROM = Console.ReadLine();

			Console.WriteLine("\nName Database To: ");
			database_TO = Console.ReadLine();

			string log = "";

			List<ErrorTableInfo> Tables_errors = new List<ErrorTableInfo>();

			int errors = 0;
			long TotalRowsAff = 0;
			long TotalRowsFound = 0;

			#endregion


			string cmdcon = "";


			//  If i want use integrated security, send User = null 

			if (String.IsNullOrEmpty(config.ConString)) cmdcon = $"Server={config.ServerName + (string.IsNullOrEmpty(config.Instance) ? "" : $"\\{config.Instance}")};Database=master;{(string.IsNullOrEmpty(config.User) ? "Trusted_Connection=True;" : $"User Id={config.User};Password={config.Password};")};";
			else cmdcon = config.ConString;

			SqlConnection con = new SqlConnection(cmdcon);

			//  Debug
			//SqlConnection con = new SqlConnection($"Server=ANALISTA2\\SQLEXPRESS;Database=master;Trusted_Connection=True;");


			//  Get the table List
			try
			{
				tableList = File.ReadAllLines("TableList.txt");
			}
			catch (Exception ex)
			{
				Console.WriteLine("(!) [ERROR 002] Ha ocurrido un error al leer la información de las tablas del proceso: \n" + ex.Message);
				Console.ReadKey();
				return;
			}

			//  Connect to DATABASE
			try
			{
				con.Open();
			}
			catch (Exception ex)
			{
				Console.WriteLine("(!) [ERROR 003] Ha ocurrido un error al conectar a la base de datos: \n" + ex.Message + "\n\nString: " + cmdcon);
				Console.ReadKey();
				return;
			}



			//  Preprocessing instructions on the new DB for prepare
			string preproc = !File.Exists("prep.sql") ? null : File.ReadAllText("prep.sql");

			if (preproc != null)
			{
				Console.WriteLine("\nExecute Preprocessing instructions (prep.sql)\n\n1) Yes\n2) No\n\nOption:");
				char opc = Console.ReadKey().KeyChar;

				if (opc == '1')
				{
					try
					{
						var cmd = new SqlCommand($"use {database_TO};\n" + preproc.Replace("[DBNEW]", database_TO).Replace("[DBOLD]", database_FROM) + "\nuse master;", con).ExecuteNonQuery();
					}
					catch (Exception ex)
					{
						Console.WriteLine("(!) [ERROR 004] Ha ocurrido un error al ejecutar las instrucciones de preprocesamiento (prep.sql): \n" + ex.Message);
						Console.WriteLine("\nContinue? \n\n1) Yes\n2) No\n\nOption: ");

						if (Console.ReadKey().KeyChar == '2') return;
					}
				}
			}
			else Console.WriteLine("(!) 'prep.sql' Not found.\n");



			//  Start the process
			for (int i = 0; i < tableList.Length; i++)
			{
				#region ReportConsole
				Console.WriteLine("\n---------------------------------------------------------------------------");
				Console.WriteLine("Processing Table: " + tableList[i]);
				log += "\n\n---------------------------------------------------------------------------\n";
				log += "Processing Table: " + tableList[i];
				#endregion

				#region Initializations

				int rowsAffected = 0;
				int rowsFound = 0;

				bool error = false;
				string message = "";

				bool hasIdentity = true;

				string columns = "";
				string PKs = "";

				#endregion


				//	Check if has a column that the other database doesn't have and update the schemes
				try
				{

					string[] columnsDBFrom = GetArrayColumnNames(con, $"{database_FROM}.[dbo].{tableList[i]}");
					string[] columnsDBTo = GetArrayColumnNames(con, $"{database_TO}.[dbo].{tableList[i]}");


					List<string> ColumnsThatDBFromNtHave = new List<string>();
					List<string> ColumnsThatDBToNtHave = new List<string>();

					GetDiffTables(out ColumnsThatDBFromNtHave,
								  out ColumnsThatDBToNtHave,
								  columnsDBFrom,
								  columnsDBTo);


					if (ColumnsThatDBFromNtHave.Count > 0)
					{

						string errorGenerateSchema = "";

						bool errorChangeSchema = GenerateSchemaDiff(ColumnsThatDBFromNtHave, tableList[i], database_FROM, database_TO, con, out errorGenerateSchema, false);

						Console.ForegroundColor = ConsoleColor.Yellow;
						Console.Write($"\nDifferent table to {database_TO} detected: ");
						log += $"\nDifferent table to {database_TO} detected: ";

						if (errorChangeSchema)
                        {
							Console.ForegroundColor = ConsoleColor.Red;
							Console.Write("Parse completed with errors: " + errorGenerateSchema + "\n");
							log += "Parse completed with errors\n";
							Console.ResetColor();

						}
						else
						{
							Console.ForegroundColor = ConsoleColor.Green;
							Console.Write("Parse completed successfully\n");
							log += "Parse completed successfully\n";
							Console.ResetColor();

						}
						Console.ResetColor();

						
					}
					if (ColumnsThatDBToNtHave.Count > 0)
                    {

						string errorGenerateSchema = "";

						bool errorChangeSchema = GenerateSchemaDiff(ColumnsThatDBToNtHave, tableList[i], database_TO, database_FROM, con, out errorGenerateSchema, true);

						Console.ForegroundColor = ConsoleColor.Yellow;
						Console.Write($"\nDifferent table to {database_FROM} detected: ");
						log += $"\nDifferent table to {database_FROM} detected: ";

						if (errorChangeSchema)
						{
							Console.ForegroundColor = ConsoleColor.Red;
							Console.Write("Parse completed with errors: "+ errorGenerateSchema + "\n");
							log += "Parse completed with errors\n";
							Console.ResetColor();

						}
						else
						{
							Console.ForegroundColor = ConsoleColor.Green;
							Console.Write("Parse completed successfully\n");
							log += "Parse completed successfully\n";
							Console.ResetColor();

						}
						Console.ResetColor();
					}

				}
				catch (Exception ex)
				{
					Console.ForegroundColor = ConsoleColor.Red;
					Console.WriteLine("The table could not be parsed: \n" + ex.Message);

					//	DEBUG
					//Console.WriteLine("trackid: " + ex.StackTrace + "\n" + ex.Source + "\n" + ex.InnerException + "\n" + ex.Data);

					log += "\nThe table could not be parsed\n";
					Console.ResetColor();
				}


				//  Check if exists rows on the table
				try
				{
					var RDTemp = new SqlCommand($"SELECT COUNT(*) FROM {database_FROM}.[dbo].{tableList[i]};", con).ExecuteReader();

					RDTemp.Read();

					rowsFound = RDTemp.GetInt32(0);

					RDTemp.Close();

					if (rowsFound == 0)
					{
						log += ReportProcessTable(error, message, rowsAffected, rowsFound);
						continue;
					}

					else TotalRowsFound += rowsFound;

				}
				catch (Exception ex)
				{
					error = true;
					message = ex.Message;
					errors++;

					Tables_errors.Add(new ErrorTableInfo(tableList[i], "0", ex.Message));

					log += ReportProcessTable(error, message, rowsAffected, rowsFound);
					continue;
				}

				//  Check if exist a PK and Get column names from the table
				try
				{
					columns = GetColumnNames(con, $"{database_FROM}.[dbo].{tableList[i]}");
					PKs = GetPK(con, tableList[i], database_TO);
				}
				catch (Exception ex)
				{
					error = true;
					message = ex.Message;
					errors++;

					Tables_errors.Add(new ErrorTableInfo(tableList[i], rowsFound.ToString(), ex.Message));

					log += ReportProcessTable(error, message, rowsAffected, rowsFound);
					continue;
				}


				//  Set the command for insert data
				string TableCommand = $"INSERT INTO {database_TO}.[dbo].{tableList[i]} ({columns}) \n" +

					$"SELECT {columns.Replace("  ", ($"{database_FROM}.[dbo].{tableList[i]}."))} \n" +

					$"FROM {database_FROM}.[dbo].{tableList[i]}";

				//  Primary Keys
				if (!string.IsNullOrEmpty(PKs))
				{
					TableCommand += $" \nWHERE NOT EXISTS ( SELECT * FROM {database_TO}.[dbo].{tableList[i]} WHERE \n";
					string[] keys = PKs.Split(',');

					for (int j = 0; j < keys.Length; j++)
					{
						if (j > 0) TableCommand += "\nAND ";

						TableCommand += keys[j] + $"!= {database_FROM}.[dbo].{tableList[i]}." + keys[j];
					}

					TableCommand += ");";


				}

				else TableCommand += ';';




				//  Verify if has Identity column
				try
				{
					var cmd = new SqlCommand($"SET IDENTITY_INSERT {database_TO}.[dbo].{tableList[i]} ON;", con);
					cmd.ExecuteNonQuery();
					hasIdentity = true;
				}
				catch (Exception ex)
				{
					if (ex.Message.Contains("no tiene la propiedad de identidad"))
						hasIdentity = false;
				}



				//  Try execute finally the transact-SQL command
				try
				{
					rowsAffected = new SqlCommand(TableCommand, con).ExecuteNonQuery();
				}
				//  Oops
				catch (Exception ex)
				{
					error = true;
					message = ex.Message;
					errors++;

					Tables_errors.Add(new ErrorTableInfo(tableList[i], rowsFound.ToString(), ex.Message));
				}
				//  Turn OFF IDENTITY_INSERT
				finally
				{
					if (hasIdentity)
					{
						try
						{
							new SqlCommand($"\nSET IDENTITY_INSERT {database_TO}.[dbo].{tableList[i]} OFF; \n", con).ExecuteNonQuery();
						}
						catch (Exception) { }
					}
				}

				TotalRowsAff += rowsAffected;
				log += ReportProcessTable(error, message, rowsAffected, rowsFound);


			}

			string textInfo = "\n_________________________________________________________________________________________________\n" +
							  "\nDone.\n" +
							  "Errors/Successful:\t" + errors + "/" + tableList.Length + "\n" +
							  "Total rows affected:\t" + TotalRowsAff + "/" + TotalRowsFound + "\n";

			Console.WriteLine(textInfo);

			log += textInfo;


			//  Postprocessing instructions on the new DB for prepare
			string posproc = !File.Exists("posp.sql") ? null : File.ReadAllText("posp.sql");

			if (posproc != null)
			{
				Console.WriteLine("\nExecute Posprocessing instructions (posp.sql)\n\n1) Yes\n2) No\n\nOption:");
				char opc = Console.ReadKey().KeyChar;

				if (opc == '1')
				{
					try
					{
						var cmd = new SqlCommand($"use {database_TO};\n" + posproc.Replace("[DBOLD]", database_FROM).Replace("[DBNEW]", database_FROM) + "\nuse master;", con).ExecuteNonQuery();

					}
					catch (Exception ex)
					{
						Console.WriteLine("(!) [ERROR 005] Ha ocurrido un error al ejecutar las instrucciones de posprocesamiento (posp.sql): \n" + ex.Message);

						//Console.WriteLine("\nDebug:--------------------------\n" +
							//$"use {database_TO};\n" + posproc.Replace("[DBOLD]", database_FROM).Replace("[DBNEW]", database_FROM) + "\nuse master;");

					}
				}
			}
			else Console.WriteLine("(!) 'posp.sql' Not found.\n"); 

			con.Close();



			char option;

			do
			{
				Console.Write("\n\n1) Only show all errors in detail\n2) Copy the log to Clipboard\n3) Generate a log.txt\n4) Exit\n\nOption: ");
				option = Console.ReadKey().KeyChar;
			} while (option != '1' && option != '2' && option != '3' && option != '4');


			if (option == '4') return;


			Console.ForegroundColor = ConsoleColor.Yellow;

			Console.WriteLine($"\n\n\nTables with error ({errors}):\n");
			log += $"\n\n\n\nTables with error ({errors}):\n";

			Console.ResetColor();

			foreach (ErrorTableInfo TableError in Tables_errors)
			{
				Console.WriteLine("\n" + TableError.Name + ": " + "0 rows of " + TableError.RowsFound);
				log += "\n\n" + TableError.Name + ": " + "0 rows of " + TableError.RowsFound;

				Console.ForegroundColor = ConsoleColor.DarkRed;

				Console.WriteLine(TableError.Message);
				log += "\n" + TableError.Message;

				Console.ResetColor();
			}

			if (option.Equals('2'))
			{
				Clipboard.SetText(log);
				Console.WriteLine("\n- Log copied to Clipboard!");
			}
			else if (option.Equals('3'))
			{
				File.WriteAllText("log.txt", log);

				Console.WriteLine("\n- Log generated into log.txt!");
			}

			Console.ReadKey();

		}

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
		private static string GetColumnNames(SqlConnection CON, string tableName)
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
		private static string[] GetArrayColumnNames(SqlConnection CON, string tableName)
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
		private static TableInfo GetTableInfo(SqlConnection con, string tableName, string databaseName)
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

			DataRows data = new DataRows(reader);

			reader.Close();

			//	PrimaryKeys
			cmd_get += $" and U.TABLE_NAME = '{tableName}';";

			reader = new SqlCommand(cmd_get, con).ExecuteReader();

			DataRows PrimaryKeys = new DataRows(reader);

			reader.Close();


			if (tableName == "Comprasub" || tableName == "Compras")
            {

            }

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

					for(int  j= 0; j < PrimaryKeys.RowCount; j++)
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
						PrimaryKeys  = isPrimaryKey ? new List<string>() : null
					});

					if (isPrimaryKey)
					{
						tableSchema.Last().PrimaryKeys.Add(namePrimaryKey);
						continue;
					}
				}
				else if (name.Equals(data.FindData("Name", i - 1)) )
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

			data = new DataRows(reader);

			reader.Close();

			List<ForeignKeyInfo> FKeys = new List<ForeignKeyInfo>();

			for(int i = 0; i < data.RowCount; i++)
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

			return new TableInfo
			{
				Columns = tableSchema,
				Foreign_Keys = FKeys.ToArray(),
				Name = tableName
			};
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


				for(int i = 0; i < columnsCount_To && hasPrimaryKeys && isDBTo; i++)
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
						if(columnsRemaining[j].Name == tableTo.Foreign_Keys[i].ColumnName)
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
				
				if(foreign_KeyRemaining.Count > 0 && isDBTo)
                {
					for(int i = 0; i < foreign_KeyRemaining.Count; i++)
                    {
						CMD_AlterTable += $"\nALTER TABLE {tablename} DROP CONSTRAINT {foreign_KeyRemaining[i][0].FK_Name};\n";
						CMD_AlterTable += $"\nALTER TABLE {tablename} ADD CONSTRAINT {foreign_KeyRemaining[i][0].FK_Name} FOREIGN KEY (";

						for (int j = 0; j < foreign_KeyRemaining[i].Count; j++)
                        {	
							if(j.Equals(0)) CMD_AlterTable += $"{tableTo.Foreign_Keys[i].ColumnName}";
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


		class ErrorTableInfo
		{
			public string Name { get; set; }
			public string RowsFound { get; set; }
			public string Message { get; set; }

			public ErrorTableInfo(string name, string rowsFound, string message)
			{
				Name = name;
				RowsFound = rowsFound;
				Message = message;
			}
		}

		class ColumnInfo
		{
			public string Name { get; set; }
			public string DefaultValue { get; set; }
			public bool IsPrimaryKey { get; set; }
			public bool IsForeignKey { get; set; }
			public string Type { get; set; }
			public bool AllowNullValues { get; set; }
			public List<string> PrimaryKeys { get; set; }


			public ColumnInfo(string name, bool primaryKey = false)
			{
				Name = name;
				IsPrimaryKey = primaryKey;
				AllowNullValues = true;
			}
			public ColumnInfo()
			{
				IsPrimaryKey = false;
				AllowNullValues = true;
			}
		}

		class ForeignKeyInfo
		{
			public string FK_Name { get; set; }
			public string TableName { get; set; }
			public string ColumnName { get; set; }
			public string ReferencedTable { get; set; }
			public string ReferencedColumn { get; set; }
		}

		class TableInfo
		{
			public string Name { get; set; }
			public List<ColumnInfo> Columns { get; set; }
			public ForeignKeyInfo[] Foreign_Keys { get; set; }
		}
	}
}
