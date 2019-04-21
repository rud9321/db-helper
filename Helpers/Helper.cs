using System;
using System.Data;
using System.Configuration;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.OleDb;
using System.Data.Odbc;
using System.Data.OracleClient;
using MySql.Data.MySqlClient;
using System.IO;
using Npgsql;
using System.Collections.Generic;

namespace Helpers
{
    public class DBHelper
    {
        #region private members
        private string _connectionstring = "";
        private DbConnection _connection;
        private DbCommand _command;
        private DbProviderFactory _factory = null;
        private DbProviders _provider;
        #endregion

        public DBHelper(string dbConnectionString)
        {
            this._connectionstring = dbConnectionString;
        }

        #region properties
        public string connectionstring
        {
            get
            {
                return _connectionstring;
            }
            set
            {
                if (value != "")
                {
                    _connectionstring = value;
                }
            }
        }
        public DbConnection connection
        {
            get
            {
                return _connection;
            }
        }
        public DbCommand command
        {
            get
            {
                return _command;
            }
        }
        #endregion
        #region methods
        public void CreateDBObjects(DbProviders providerList)
        {
            _provider = providerList;
            switch (providerList)
            {
                case DbProviders.SqlServer:
                    _factory = SqlClientFactory.Instance;
                    break;
                //case DbProviders.Oracle:
                //    _factory = OracleClientFactory.Instance;
                //    break;
                //case DbProviders.OleDb:
                //   _factory = OleDbFactory.Instance;
                //    break;
                //case DbProviders.ODBC:
                //    _factory = OdbcFactory.Instance;
                //    break;
                case DbProviders.MySql:
                    _factory = MySqlClientFactory.Instance;
                    break;
                case DbProviders.NpgSql:
                    _factory = NpgsqlFactory.Instance;
                    break;
            }
            _connection = _factory.CreateConnection();
            _connection.ConnectionString = _connectionstring; //connectString;
            _command = _factory.CreateCommand();
            _command.Connection = connection;
        }
        #region parameters
        public int AddParameter(string name, object value)
        {
            DbParameter parm = _factory.CreateParameter();
            parm.ParameterName = name;
            parm.Value = value;
            return command.Parameters.Add(parm);
        }
        public int AddParameter(DbParameter parameter)
        {
            return command.Parameters.Add(parameter);
        }
        public void ClearParameter()
        {
            if (_command != null)
            {
                if (_command.Parameters.Count > 0)
                {
                    _command.Parameters.Clear();
                }
            }
        }
        #endregion
        #region transactions
        private void BeginTransaction()
        {
            if (connection.State == System.Data.ConnectionState.Closed)
            {
                connection.Open();
            }
            command.Transaction = connection.BeginTransaction();
        }
        private void CommitTransaction()
        {
            command.Transaction.Commit();
            connection.Close();
        }
        private void RollbackTransaction()
        {
            command.Transaction.Rollback();
            connection.Close();
        }
        #endregion
        #region execute database functions
        public int ExecuteNonQuery(string query, CommandType commandtype, ConnectionState connectionstate)
        {
            command.CommandText = query;
            command.CommandType = commandtype;
            int i = -1;
            try
            {
                if (connection.State == System.Data.ConnectionState.Closed)
                {
                    connection.Open();
                }
                BeginTransaction();
                i = command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                RollbackTransaction();
                throw (ex);
            }
            finally
            {
                CommitTransaction();
                command.Parameters.Clear();
                if (connection.State == System.Data.ConnectionState.Open)
                {
                    connection.Close();
                    connection.Dispose();
                    //command.Dispose();
                }
            }
            return i;
        }

        public object ExecuteScaler(string query)//, CommandType commandtype, ConnectionState connectionstate)
        {
            command.CommandText = query;
            command.CommandType = CommandType.Text;
            object obj = null;
            try
            {
                if (connection.State == System.Data.ConnectionState.Closed)
                {
                    connection.Open();
                }
                BeginTransaction();
                obj = command.ExecuteScalar();
            }
            catch (Exception ex)
            {
                RollbackTransaction();
                throw (ex);
            }
            finally
            {
                CommitTransaction();
                command.Parameters.Clear();
                if (connection.State == System.Data.ConnectionState.Open)
                {
                    connection.Close();
                    connection.Dispose();
                    command.Dispose();
                }
            }
            return obj;
        }
        public DbDataReader ExecuteReader(string query)//, CommandType commandtype, ConnectionState connectionstate)
        {
            command.CommandText = query;
            command.CommandType = CommandType.Text;
            DbDataReader reader = null;
            try
            {
                if (connection.State == System.Data.ConnectionState.Closed)
                {
                    connection.Open();
                }
                if (connection.State == System.Data.ConnectionState.Open)
                {
                    reader = command.ExecuteReader(CommandBehavior.CloseConnection);
                }
                else
                {
                    reader = command.ExecuteReader();
                }
            }
            catch (Exception ex)
            {
                throw (ex);
            }
            finally
            {
                command.Parameters.Clear();

            }
            return reader;
        }
        public DataSet GetDataSet(string query, CommandType commandtype, ConnectionState connectionstate)
        {
            DbDataAdapter adapter = _factory.CreateDataAdapter();
            command.CommandText = query;
            command.CommandType = commandtype;
            adapter.SelectCommand = command;
            DataSet ds = new DataSet();
            try
            {
                adapter.Fill(ds);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                command.Parameters.Clear();
                if (connection.State == System.Data.ConnectionState.Open)
                {
                    connection.Close();
                    connection.Dispose();
                    command.Dispose();
                }
            }
            return ds;
        }
        public DataSet ExecuteDataSetRefCur(NpgsqlConnection conn, NpgsqlCommand ObjDatabaseHelper, string isCursor)
        {
            DataSet ds = new DataSet();
            DataSet dsCurDet = new DataSet();
            conn.Open();
            var tran = conn.BeginTransaction();
            var cmd = ObjDatabaseHelper;
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandTimeout = 0;
            try
            {
                NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd);
                da.Fill(ds);
                if (ds.Tables.Count > 0)
                {
                    if (isCursor.ToUpper().Trim().Equals("Y"))
                    {
                        foreach (DataRow r in ds.Tables[0].Rows)
                        {
                            DataTable dt = new DataTable();
                            using (cmd = new NpgsqlCommand("FETCH ALL IN " + "\"" + r[0].ToString() + "\"", conn))
                            {
                                NpgsqlDataAdapter daa = new NpgsqlDataAdapter(cmd);
                                daa.Fill(dt);
                                dsCurDet.Tables.Add(dt);
                            }
                        }
                        tran.Commit();
                        conn.Close();
                    }
                    else
                    {
                        dsCurDet = ds;
                        tran.Commit();
                    }
                }
                else
                {
                    dsCurDet = null;
                }
            }
            catch (Exception ex)
            {
                if (tran != null) tran.Rollback();
                throw ex;
            }
            finally
            {
                if (conn != null) conn.Close();
            }
            return dsCurDet;
        }
        #endregion
        #endregion
        #region enums
        public enum DbProviders
        {
            SqlServer, OleDb, Oracle, ODBC, MySql, SQLite, NpgSql
        }
        #endregion
    }
}