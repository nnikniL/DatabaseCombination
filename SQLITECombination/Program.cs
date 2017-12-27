using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.IO;
using System.Data;

namespace SQLITECombination
{
    class Program
    {
        const string strTableStructQSQL = "SELECT * FROM sqlite_master WHERE type = \"table\"";
        const string strConsoleSucc = "[Success]\t";
        const string strConsoleFail = "[Fail]\t";
        const string strOOMFail = "Out Of Memory!";
        static void MsgOut(string msg, bool isSucceed)
        {
            Console.WriteLine((isSucceed? strConsoleSucc:strConsoleFail) + msg);
        }
        static void Main(string[] args)
        {
            //目标数据库
            SQLiteConnection connDst = new SQLiteConnection();
            if (connDst == null)
                return;
            SQLiteConnectionStringBuilder sqlstrDst = new SQLiteConnectionStringBuilder();
            if (sqlstrDst == null)
                return;
            //枚举所有db文件
            FileInfo[] files =
                (new DirectoryInfo(Directory.GetCurrentDirectory())).GetFiles("*.db");
            //创建目标数据库
            string datasourceDst = "combi.db";
            try
            {
				//覆盖创建
                SQLiteConnection.CreateFile(datasourceDst);
            }
            catch (Exception e)
            {
                MsgOut(e.Message,false);
                Console.ReadKey();
                return;

            }
            //数据库连接
            sqlstrDst.DataSource = datasourceDst;
            connDst.ConnectionString = sqlstrDst.ToString();
            connDst.Open();
            //查询表结构语句
            SQLiteCommand cmdStructQuery = new SQLiteCommand(strTableStructQSQL);
            List<string> lstTables = new List<string>(8);   //表名存储

            DataTable dTable = new DataTable();
            if (dTable == null)
            {
                MsgOut(strOOMFail, false);
                return;
            }

            //迭代每个数据库文件
            int totalNum = 0;
            for (int filei = 0; filei<files.Length;filei++)
            {
                if (files[filei].Name == datasourceDst)
                    continue;
                //源数据库
                SQLiteConnection connSrc = new SQLiteConnection();
                if (connSrc == null)
                    continue;
                SQLiteConnectionStringBuilder sqlstrSrc = new SQLiteConnectionStringBuilder();
                if (sqlstrSrc == null)
                    continue;
                sqlstrSrc.DataSource = files[filei].Name;
                connSrc.ConnectionString = sqlstrSrc.ToString();
                connSrc.Open();
                
                //查询表结构并创建
                cmdStructQuery.Connection = connSrc;
                dTable = connSrc.GetSchema("TABLES");
                //不存在表结构
                if (dTable.Rows.Count<=0 
                    || dTable.Columns.Contains("TABLE_NAME")==false
                    || dTable.Columns.Contains("TABLE_DEFINITION")==false)
                {
                    continue;
                }
                dTable.Columns["TABLE_NAME"].SetOrdinal(0);			//表名
                dTable.Columns["TABLE_DEFINITION"].SetOrdinal(1);	//表定义式，就是SQL语句
           
                //遍历每张表
                DataTable querydata = new DataTable();
                if (querydata == null)
                    continue;
                for (int rowsi = 0; rowsi < dTable.Rows.Count; rowsi++)
                {
                    string tablename = dTable.Rows[rowsi][0].ToString();
                    if (tablename.Length == 0)
                        continue;
                    string strsql = "select * from " + tablename;
                    if (tablename.ToLower().IndexOf("sqlite_")>=0)  //遇到sqlite的预留表名
                        continue;
                    if (false == lstTables.Contains(tablename)) 
                    {   //如果还没有创建该表，那么创建一下
                        SQLiteCommand cmdTabelCreateDst = new SQLiteCommand(dTable.Rows[rowsi][1].ToString(), connDst);
                        try //建表
                        {
                            int nRet = cmdTabelCreateDst.ExecuteNonQuery();
                        }
                        catch (Exception e)
                        {
							//如果为已存在表，则忽略
                            if (e.Message.IndexOf("already exists")<0)
                            {
                                throw e;
                            }
                        }
                        finally
                        {
                            lstTables.Add(tablename);
                        }
                    }
                    //查询该表数据
                    querydata.Clear();
                    SQLiteDataAdapter dataAdapterSrc;
                    try
                    {
                        dataAdapterSrc = new SQLiteDataAdapter(strsql, connSrc);
                        if (dataAdapterSrc == null) { MsgOut(strOOMFail, false); continue; }
                        dataAdapterSrc.AcceptChangesDuringFill = false;
                        dataAdapterSrc.Fill(querydata);
                    }
                    catch (Exception ex)
                    {
                        MsgOut(ex.Message, false);
                        continue;
                    }
                   
                    //并写入新数据库中
                    Console.WriteLine("To insert data, its count = " + querydata.Rows.Count);
                    totalNum += querydata.Rows.Count;
                    SQLiteTransaction trans; SQLiteCommand cmdDst; SQLiteDataAdapter dataAdapterDst; SQLiteCommandBuilder builderDst;
                    try
                    {
                        trans = connDst.BeginTransaction();
                        cmdDst = new SQLiteCommand(strsql, connDst, trans);
                        if (cmdDst == null) { MsgOut(strOOMFail, false); continue; }
                        dataAdapterDst = new SQLiteDataAdapter(cmdDst);
                        if (dataAdapterDst == null) { MsgOut(strOOMFail, false); continue; }
                        builderDst = new SQLiteCommandBuilder(dataAdapterDst);
                        if (builderDst == null) { MsgOut(strOOMFail, false); continue; }
                        dataAdapterDst.InsertCommand = builderDst.GetInsertCommand();   //下方Parameters需要用到，不执行这句无法生成Parameters
                    }
                    catch (Exception ex)
                    {
                        MsgOut(ex.Message, false);
                        continue;
                    }
                    #region 去除主键字段
                    //获取主键列表
                    DataTable tableInfo = connSrc.GetSchema("IndexColumns", new string[] { null, null, tablename });
                    for (int indi = 0; indi < tableInfo.Rows.Count; indi++)
                    {
                        for (int parami = 0; parami < builderDst.DataAdapter.InsertCommand.Parameters.Count; parami++)
                        {
                            if (builderDst.DataAdapter.InsertCommand.Parameters[parami].SourceColumn == tableInfo.Rows[indi].ItemArray[6].ToString())
                            {
                                builderDst.DataAdapter.InsertCommand.Parameters.RemoveAt(parami);
                                parami--;
                                break;
                            }
                        }

                    }
                    #endregion
                    #region 根据参数重新构造语句并写入数据库
                    Console.WriteLine("start insert from table " + tablename + " of file "+ files[filei].Name+
                        " to dst @ " + DateTime.Now.ToString("yyyyMMdd hhmmss"));
                    //逐个生成sql语句并写入目标数据库
                    foreach (DataRow row in querydata.Rows)
                    {
                        SQLiteCommand cmdInsertDstSub = new SQLiteCommand(BuildSubInsertSQL(builderDst, tablename, row), connDst);
                        if (cmdInsertDstSub == null)
                            continue;
                        int nRetSub = cmdInsertDstSub.ExecuteNonQuery();
                    }
                    trans.Commit();
                    Console.WriteLine("stop insert @" + DateTime.Now.ToString("yyyyMMdd hhmmss"));
                    #endregion

                }
                connSrc.Close();
            }
            connDst.Close();
            MsgOut("Finished:"+"Total="+totalNum, true);
            Console.ReadKey();
            
        }
        static string BuildSubInsertSQL(SQLiteCommandBuilder thisbuilder, string tablename, DataRow datarow)
        {
            StringBuilder builder = new StringBuilder();
            StringBuilder builderForValues = new StringBuilder();
            int index = 0;
            string str2 = " (";
            builder.Append("INSERT INTO ");
            builder.Append(thisbuilder.QuotePrefix + tablename + thisbuilder.QuoteSuffix);
            builderForValues.Append(" VALUES");
            SQLiteParameterCollection ColumnsToWrite = thisbuilder.DataAdapter.InsertCommand.Parameters;
            string[] strArray = new string[ColumnsToWrite.Count];
            for (int i = 0; i < ColumnsToWrite.Count; i++)
            {
                SQLiteParameter row = ColumnsToWrite[i];
                if (((row != null) && (row.SourceColumn.Length != 0)))//父类补充条件：&& thisbuilder.IncludeInInsertValues(row))
                {
                    string columnName = row.SourceColumn;
                    builder.Append(str2);
                    builderForValues.Append(str2);
                    str2 = ", ";
                    builder.Append(thisbuilder.QuotePrefix + row.SourceColumn + thisbuilder.QuoteSuffix);

                    //values build
                    builderForValues.Append("'");
                    builderForValues.Append(datarow[columnName].ToString());
                    builderForValues.Append("'");

                    index++;
                }
            }
            builder.Append(")");
            builderForValues.Append(")");
            if (index == 0)
            {
                builder.Append(" DEFAULT VALUES");
            }
            else
            {
                builder.Append(builderForValues);
            }            
            return builder.ToString();

        }
        static string BuildInsertSQL(SQLiteCommandBuilder thisbuilder,string tablename)
        {
            StringBuilder builder = new StringBuilder();
            int index = 0;
            string str2 = " (";
            builder.Append("INSERT INTO ");
            builder.Append(thisbuilder.QuotePrefix+ tablename+ thisbuilder.QuoteSuffix);
            SQLiteParameterCollection rowArray = thisbuilder.DataAdapter.InsertCommand.Parameters;
            string[] strArray = new string[rowArray.Count];
            for (int i = 0; i < rowArray.Count; i++)
            {
                SQLiteParameter row = rowArray[i];
                if (((row != null) && (row.SourceColumn.Length != 0)) )//父类补充条件：&& thisbuilder.IncludeInInsertValues(row))
                {
                    string columnName = row.SourceColumn;
                    builder.Append(str2);
                    str2 = ", ";
                    builder.Append(thisbuilder.QuotePrefix + row.SourceColumn + thisbuilder.QuoteSuffix);
                    index++;
                }
            }
            if (index == 0)
            {
                builder.Append(" DEFAULT VALUES");
            }
            else
            {
                builder.Append(")");
                builder.Append(" VALUES ");
                builder.Append("(");
                builder.Append(rowArray[0].ParameterName);
                for (int j = 1; j < index; j++)
                {
                    builder.Append(", ");
                    builder.Append(rowArray[j].ParameterName);
                }
                builder.Append(")");
            }
            //command.CommandText = builder.ToString();
            //RemoveExtraParameters(command, index);
            //this.InsertCommand = command;
            //return command;
            return builder.ToString();

        }
    }
}
