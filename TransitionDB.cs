using System;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;
using System.IO;
using System.Threading;

namespace PerseToolkit
{
	public partial class TransitionDB
	{
		[STAThread]
		public static void TransitionDatabases(Config config)
		{

			#region Initializations

			string[] tableList = new string[0];

			string database_FROM = "BPerseDATO";
			string database_TO = "BPerse";


			Console.WriteLine("Name source database: ");
			database_FROM = Console.ReadLine(); //	DEBUG

			Console.WriteLine("\nName target database: ");
			database_TO = Console.ReadLine();	//	DEBUG

			string log = "";

			List<ErrorTableInfo> Tables_errors = new List<ErrorTableInfo>();

			int errors = 0;
			long TotalRowsAff = 0;
			long TotalRowsFound = 0;

			int mode = -1;

			int[] modes = { 1, 2, 3};

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

			//	Menu to select options
			mode = SetMode(modes);


			//  Preprocessing instructions on the new DB for prepare
			string preproc = !File.Exists("prep.sql") ? null : File.ReadAllText("prep.sql");

			if (preproc != null)
			{
				Console.WriteLine("\n\n\nExecute Preprocessing instructions (prep.sql)\n\n1) Yes\n2) No\n\nOption:");
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



			bool processing = true;

			Console.CancelKeyPress += delegate (object sender, ConsoleCancelEventArgs e) 
			{
				processing = false;
				e.Cancel = true;
				string txt = "\n(!) Operation canceled.\n";
				Console.WriteLine(txt);
				log += $"\n{txt}\n";
			};

			Console.WriteLine("\nInitializing...\n");
			Thread.Sleep(3000);
			
			//Thread.Sleep(2000);	//	time to cancel operation	:(



			//  Start the process
			for (int i = 0; i < tableList.Length && processing; i++)
			{


				//	Execute main process
				ProcessTable(ref log,
							 ref errors,
							 ref TotalRowsAff,
							 ref TotalRowsFound,
							 ref Tables_errors,
							 database_FROM,
							 database_TO,
							 mode,
							 con,
							 tableList[i]);


			}

			string textInfo = "\n_________________________________________________________________________________________________\n" +
							  (processing ? "\nDone.\n" : "\nCanceled\n") +
							  "Errors/Successful:\t" + errors + "/" + tableList.Length + "\n" +
							  "Total rows affected:\t" + TotalRowsAff + "/" + TotalRowsFound + "\n";

			Console.WriteLine(textInfo);

			log += textInfo;


			//  Postprocessing instructions on the new DB for prepare
			string posproc = !File.Exists("posp.sql") ? null : File.ReadAllText("posp.sql");

			if (posproc != null)
			{
				Console.WriteLine("\nExecute Postprocessing instructions (posp.sql)\n\n1) Yes\n2) No\n\nOption:");
				char opc = Console.ReadKey().KeyChar;

				if (opc == '1')
				{
					try
					{
						var cmd = new SqlCommand($"use {database_TO};\n" + posproc.Replace("[DBOLD]", database_FROM).Replace("[DBNEW]", database_FROM) + "\nuse master;", con).ExecuteNonQuery();

					}
					catch (Exception ex)
					{
						Console.WriteLine("(!) [ERROR 005] Ha ocurrido un error al ejecutar las instrucciones de postprocesamiento (posp.sql): \n" + ex.Message);

						//Console.WriteLine("\nDebug:--------------------------\n" +
							//$"use {database_TO};\n" + posproc.Replace("[DBOLD]", database_FROM).Replace("[DBNEW]", database_FROM) + "\nuse master;");

					}
				}
			}
			else Console.WriteLine("(!) 'posp.sql' Not found.\n");

			//con.Close();



			char option;

            while (true)
            {
				//Console.Clear();
				option = OptionsFinal();

				if (option == '9') return;

				else if (option == '4' || option == '5')
                {
					string[] tablesFrom = GetAllTables(con, database_FROM);
					string[] tablesTo = GetAllTables(con, database_TO);

					List<string> tablesNotProcessed = new List<string>();
					List<string> tablesThatOldNotHave = new List<string>();
					List<string> tablesThatNewNotHave = new List<string>();


					//	Flag
					bool founded;


					// Tables not processed (from old)
					for (int i = 0; i < tablesFrom.Length; i++)
                    {
						founded = false;

						for(int j = 0; j < tableList.Length; j++)
                        {
							if(tableList[j] == tablesFrom[i])
                            {
								founded = true;
								break;
                            }
                        }

						if (!founded) tablesNotProcessed.Add(tablesFrom[i]);
					}

					// Tables not processed (from new)
					for (int i = 0; i < tablesTo.Length; i++)
					{
						founded = false;

						for (int j = 0; j < tableList.Length; j++)
						{
							if (tableList[j] == tablesTo[i])
							{
								founded = true;
								break;
							}
						}

						if (!founded)
                        {
							if((from str in tablesNotProcessed where !str.Equals(tablesTo[i]) select str).ToArray().Length < 1) 
								tablesNotProcessed.Add(tablesTo[i]);
						}
					}

					//---

					//	Tables that Old database not have
					for (int i = 0; i < tablesTo.Length; i++)
					{
						founded = false;

						for (int j = 0; j < tablesFrom.Length; j++)
						{
							if (tablesTo[i] == tablesFrom[j])
							{
								founded = true;
								break;
							}
						}

						if (!founded) tablesThatOldNotHave.Add(tablesTo[i]);
					}

					//	Tables that New database not have
					for (int i = 0; i < tablesFrom.Length; i++)
					{
						founded = false;

						for (int j = 0; j < tablesTo.Length; j++)
						{
							if (tablesTo[j] == tablesFrom[i])
							{
								founded = true;
								break;
							}
						}

						if (!founded) tablesThatNewNotHave.Add(tablesFrom[i]);
					}

					//---

					//	Report
					/*
					Console.ForegroundColor = ConsoleColor.White;
					Console.WriteLine("\nTables not processed:\n");
					log += "\n\nTables not processed:\n\n";
					Console.ResetColor();
					foreach(string table in tablesNotProcessed)
                    {
						Console.WriteLine("- " + table);
						log += "\n- " + table + "\n";

					}
					*/

					Console.ForegroundColor = ConsoleColor.White;
					Console.WriteLine("\nTables remaining detected: " + tablesNotProcessed.Count);
					log += "\n\nTables remaining detected: " + tablesNotProcessed.Count + "\n";
					Console.ResetColor();

					Console.ForegroundColor = ConsoleColor.Yellow;
					Console.WriteLine("\nTables that Old database not have:\n");
					Console.ResetColor();
					foreach (string table in tablesThatOldNotHave)
					{
						Console.WriteLine("- " + table);
						log += "\n- " + table + "\n";
					}

					Console.ForegroundColor = ConsoleColor.Yellow;
					Console.WriteLine("\nTables that New database not have:\n");
					Console.ResetColor();
					foreach (string table in tablesThatNewNotHave)
					{
						Console.WriteLine("- " + table);
						log += "\n- " + table + "\n";
					}

					if(option == '4')
                    {
						File.WriteAllText("log.txt", log);
						continue;
					}



					//	Else opc == 5...
					Console.ForegroundColor = ConsoleColor.Yellow;
					Console.Write("\n\n(!) Execute table process with remaining tables.\n");
					Console.ResetColor();

					mode = SetMode(modes);

					int rem_Errors = 0;
					long rem_TotalRowsAff = 0;
					long rem_TotalRowsFound = 0;

					processing = true;

					for (int i = 0; i < tablesNotProcessed.Count && processing; i++)
                    {
						//	Execute 
						ProcessTable(ref log,
									 ref rem_Errors,
									 ref rem_TotalRowsAff,
									 ref rem_TotalRowsFound,
									 ref Tables_errors,
									 database_FROM,
									 database_TO,
									 mode,
									 con,
									 tablesNotProcessed[i]);
					}

					string textInfo_Rem = "\n_________________________________________________________________________________________________\n" +
							  "\nDone.\n" +
							  "Errors/Successful:\t" + rem_Errors + "/" + tablesNotProcessed.Count + "\n" +
							  "Total rows affected:\t" + rem_TotalRowsAff + "/" + rem_TotalRowsFound + "\n";

					log += "\n" + textInfo_Rem + "\n";
					Console.WriteLine(textInfo_Rem);



					File.WriteAllText("log.txt", log);

				}

				else if (option == '1' || option == '2' || option == '3')
				{
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

			}


			

		}
		private static char OptionsFinal()
        {
			char option;
			do
			{
				Console.Write("\n\n1) Only show all errors in detail\n" +
								  "2) Copy the log to Clipboard\n" +
								  "3) Generate a log.txt\n" +
								  "4) Show detailed info about tables\n" +
								  "5) Try to process remaining tables\n" +
								  "9) Exit\n\n" +
								  "Option: ");
				option = Console.ReadKey().KeyChar;
			} while (option != '1' && option != '2' && option != '3' && option != '4' && option != '5' && option != '9');
			return option;
		}

		private static int SetMode(int[] modes)
        {
			int mode = -1;
			do
			{

				Console.Write("\n\nModes:\n\n" +
					"1.- FULL Transition (DATA and SCHEMES):  " +
					"\n\tA -> B (Data)" +
					"\n\tA -> B (Scheme)" +
					"\n\tA <- B (Scheme to avoid mistakes)" +
					"\n\t<Make backup of 'from' database if you want preserve his old scheme>.\n\n" +
					"2.- Only data on tables (try):" +
					"\n\tA -> B (Data)\n\n" +
					"3.- Only shemes (Add fields and keys)" +
					"\n\tA -> B (Scheme)" +
					"\n\t<Make backup of 'from' database if you want preserve his old scheme>.\n\n\nMode: ");

				try
				{
					mode = Convert.ToInt32(Console.ReadKey().KeyChar.ToString());
				}
				catch (Exception) { Console.Clear(); continue; }


			} while (!modes.Contains(mode));

			return mode;
		}

		/// <summary>
		/// Subproceso que realiza la tarea principal del algoritmo para procesar las tablas
		/// </summary>
		private static void ProcessTable(ref string log,
										 ref int errors,
										 ref long TotalRowsAff,
										 ref long TotalRowsFound,
										 ref List<ErrorTableInfo> Tables_errors,
										 string database_FROM,
										 string database_TO,
										 int mode,
										 SqlConnection con,
										 string tableName)
        {
			#region ReportConsole
			Console.WriteLine("\n---------------------------------------------------------------------------");
			Console.WriteLine("Processing Table: " + tableName);
			log += "\n\n---------------------------------------------------------------------------\n";
			log += "Processing Table: " + tableName;
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


			//	Check if has a column that the other database doesn't have and update the schemes (if mode != 2)
			try
			{

				string[] columnsDBFrom = GetArrayColumnNames(con, $"{database_FROM}.[dbo].{tableName}");
				string[] columnsDBTo = GetArrayColumnNames(con, $"{database_TO}.[dbo].{tableName}");


				List<string> ColumnsThatDBFromNtHave = new List<string>();
				List<string> ColumnsThatDBToNtHave = new List<string>();

				GetDiffTables(out ColumnsThatDBFromNtHave,
							  out ColumnsThatDBToNtHave,
							  columnsDBFrom,
							  columnsDBTo);


				do
				{
					if (ColumnsThatDBFromNtHave.Count > 0)
					{

						string errorGenerateSchema = "";

						Console.ForegroundColor = ConsoleColor.Yellow;
						Console.Write($"\nDifferent table to {database_TO} detected: ");
						log += $"\nDifferent table to {database_TO} detected: ";


						if (mode == 2)
						{
							Console.ForegroundColor = ConsoleColor.DarkYellow;
							Console.Write("Warning <Mode only data> (!).\n");
							log += "Warning <Mode only data> (!).\n";
							break;
						}

						bool errorChangeSchema = GenerateSchemaDiff(ColumnsThatDBFromNtHave, tableName, database_FROM, database_TO, con, out errorGenerateSchema, false);



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

				} while (false);

				do
				{
					if (ColumnsThatDBToNtHave.Count > 0)
					{

						string errorGenerateSchema = "";


						Console.ForegroundColor = ConsoleColor.Yellow;
						Console.Write($"\nDifferent table to {database_FROM} detected: ");
						log += $"\nDifferent table to {database_FROM} detected: ";


						if (mode == 2)
						{
							Console.ForegroundColor = ConsoleColor.DarkYellow;
							Console.Write("Warning <Mode only data> (!).\n");
							log += "Warning <Mode only data> (!).\n";
							break;
						}

						bool errorChangeSchema = GenerateSchemaDiff(ColumnsThatDBToNtHave, tableName, database_TO, database_FROM, con, out errorGenerateSchema, true);



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

				} while (false);

			}
			catch (Exception ex)
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine("The table could not be parsed: \n" + ex.Message);


				log += "\nThe table could not be parsed\n";
				Console.ResetColor();
			}

			//	DATA
			if (mode == 1 || mode == 2)
			{
				//  Check if exists rows on the table
				try
				{
					var RDTemp = new SqlCommand($"SELECT COUNT(*) FROM {database_FROM}.[dbo].{tableName};", con).ExecuteReader();

					RDTemp.Read();

					rowsFound = RDTemp.GetInt32(0);

					RDTemp.Close();

					if (rowsFound == 0)
					{
						log += ReportProcessTable(error, message, rowsAffected, rowsFound);
						return;
					}

					else TotalRowsFound += rowsFound;

				}
				catch (Exception ex)
				{
					error = true;
					message = ex.Message;
					errors++;

					Tables_errors.Add(new ErrorTableInfo(tableName, "0", ex.Message));

					log += ReportProcessTable(error, message, rowsAffected, rowsFound);
					return;
				}


				//  Check if exist a PK and Get column names from the table
				try
				{
					columns = GetColumnNames(con, $"{database_FROM}.[dbo].{tableName}");
					PKs = GetPK(con, tableName, database_TO);
				}
				catch (Exception ex)
				{
					error = true;
					message = ex.Message;
					errors++;

					Tables_errors.Add(new ErrorTableInfo(tableName, rowsFound.ToString(), ex.Message));

					log += ReportProcessTable(error, message, rowsAffected, rowsFound);
					return;
				}


				//  Set the command for insert data
				string TableCommand = $"INSERT INTO {database_TO}.[dbo].{tableName} ({columns}) \n" +

					$"SELECT {columns.Replace("  ", ($"{database_FROM}.[dbo].{tableName}."))} \n" +

					$"FROM {database_FROM}.[dbo].{tableName}";

				//  Primary Keys
				if (!string.IsNullOrEmpty(PKs))
				{
					TableCommand += $" \nWHERE NOT EXISTS ( SELECT * FROM {database_TO}.[dbo].{tableName} WHERE \n";
					string[] keys = PKs.Split(',');

					for (int j = 0; j < keys.Length; j++)
					{
						if (j > 0) TableCommand += "\nAND ";

						TableCommand += keys[j] + $"!= {database_FROM}.[dbo].{tableName}." + keys[j];
					}

					TableCommand += ");";


				}

				else TableCommand += ';';




				//  Verify if has Identity column
				try
				{
					var cmd = new SqlCommand($"SET IDENTITY_INSERT {database_TO}.[dbo].{tableName} ON;", con);
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
					var cmd = new SqlCommand(TableCommand, con);
					cmd.CommandTimeout = 300;
					rowsAffected = new SqlCommand(TableCommand, con).ExecuteNonQuery();
				}
				//  Oops
				catch (Exception ex)
				{
					error = true;
					message = ex.Message;
					errors++;

					Tables_errors.Add(new ErrorTableInfo(tableName, rowsFound.ToString(), ex.Message));
				}
				//  Turn OFF IDENTITY_INSERT
				finally
				{
					if (hasIdentity)
					{
						try
						{
							new SqlCommand($"\nSET IDENTITY_INSERT {database_TO}.[dbo].{tableName} OFF; \n", con).ExecuteNonQuery();
						}
						catch (Exception) { }
					}
				}

				TotalRowsAff += rowsAffected;
				log += ReportProcessTable(error, message, rowsAffected, rowsFound);

			}
		}
	}
}
