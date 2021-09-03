using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PerseToolkit
{
    public partial class TransitionDB
    {

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

		
	}
	public class ColumnInfo
	{
		public string Name { get; set; }
		public string DefaultValue { get; set; }
		public bool IsPrimaryKey { get; set; }
		public bool IsForeignKey { get; set; }
		public bool IsIdentity { get; set; }
		public string Type { get; set; }
		public bool AllowNullValues { get; set; }
		public List<string> PrimaryKeys { get; set; }


		public ColumnInfo(string name, bool primaryKey = false)
		{
			Name = name;
			IsPrimaryKey = primaryKey;
			AllowNullValues = true;
			IsIdentity = false;
		}
		public ColumnInfo()
		{
			IsPrimaryKey = false;
			AllowNullValues = true;
			IsIdentity = false;
		}
	}

	public class ForeignKeyInfo
	{
		public string FK_Name { get; set; }
		public string TableName { get; set; }
		public string ColumnName { get; set; }
		public string ReferencedTable { get; set; }
		public string ReferencedColumn { get; set; }
	}

	public class TableInfo
	{
		public string Name { get; set; }
		public List<ColumnInfo> Columns { get; set; }
		public ForeignKeyInfo[] Foreign_Keys { get; set; }
	}
}
