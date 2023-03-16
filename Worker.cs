using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace WorkAccionesBBVA
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var now = DateTime.Now;
                var startOfMonth = new DateTime(now.Year, now.Month, 1);
                var DaysInMonth = DateTime.DaysInMonth(now.Year, now.Month);
                var lastDay = new DateTime(now.Year, now.Month, DaysInMonth);
                //using (SqlConnection connection = new SqlConnection(@"Data Source=localhost,1401;Initial catalog=SERVERPROD;User ID=sa;Password=test@123;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False"))
                //using (SqlConnection connection = new SqlConnection(@"Data Source=localhost,1401;Initial catalog=SERVERPROD;User ID=leonidas;Password=leonidas12345678910-;Connect Timeout=30;Encrypt=False;TrustServerCertificate=False;ApplicationIntent=ReadWrite;MultiSubnetFailover=False"))
                //using (SqlConnection connection = new SqlConnection(@"Data Source=ITGDESAOCSRV.andreani.com.ar;Initial catalog=AccionesBBVA;Integrated Security=true"))
                //using (SqlConnection connection = new SqlConnection(@"Data Source=DBSCEFARMATEST;Initial catalog=LPNFD;Integrated Security=true"))
                {
                    //connection.Open();
                    //SqlDataReader sqlDr = null;
                    string queryString = "SELECT SUBSTRING(er.Registro,3,21) + SUBSTRING(er.Registro,85,3) + SUBSTRING(er.Registro,94,180)" +
                        "FROM AccionesBBVA..EntradaRegistros (nolock) ER " +
                        "where ER.codigoAccion IN (" +
                        "'003'," +
                        "'006'," +
                        "'016'" +
                        ")" +
                        "and er.fechaCreacion Between '2020-01-08 00:00:00' and '2022-12-09 00:00:00' " +
                        "and ER.Respuesta IN (" +
                        "4" +
                        ")" +
                        "and" +
                        "(" +
                        "er.observaciones like 'El estado del envio no permite realizar la operación. Envío en estado final. Estado: 6' " +
                        "OR  er.observaciones like 'El estado del envio no permite realizar la operación. Envío en estado final. Estado: 7' " +
                        "OR  er.observaciones like 'El estado del envio no permite realizar la operación. Envío en estado final. Estado: 8'" +
                        "OR  er.observaciones like 'No se pudo hallar el objeto: EntityNumber con identificador: G00000576967660'" +
                        ") " +
                        "AND not exists (" +
                        "select 1 from AccionesBBVA..EntradaRegistros (nolock) ER2 " +
                        "where " +
                        "er.NumeroInterno = er2.NumeroInterno " +
                        "and er2.respuesta <> 4 " +
                        "and er.codigoAccion = er2.codigoAccion " +
                        "and er.fechaCreacion < er2.fechaCreacion " +
                        "and er.id <> er2.id" +
                        ")";
                    string connectionString = @"Data Source=ITGTESTDCSRV1.andreani.com.ar;Initial Catalog=AccionesBBVA;Persist Security Info=True;User ID=Andreani_test;Password=cualquiercosa";
                    DataTable dt = new DataTable();
                    int rows_returned;

                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        SqlCommand command = new SqlCommand(queryString, connection);
                        //command.Parameters.AddWithValue("@tPatSName", "Your-Parm-Value");
                        using (SqlDataAdapter sda = new SqlDataAdapter(command))
                        {
                            command.CommandText = queryString;
                            command.CommandType = CommandType.Text;
                            connection.Open();
                            rows_returned = sda.Fill(dt);
                            connection.Close();
                        }
                        if (dt.Rows.Count > 0)
                        {
                            foreach (var item in dt.AsEnumerable())
                            {
                                Console.WriteLine(item.ItemArray[0]);
                            }
                        }
                        else
                        {
                           
                           
                        }
                    }

                    //using (SqlCommand cmd = new SqlCommand("sp_AccionesBBVA", connection))
                    //{
                    //    cmd.CommandType = CommandType.StoredProcedure;
                    //    cmd.CommandTimeout = 0;

                    //    using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                    //    {
                    //        DataTable dt = new DataTable();

                    //        da.Fill(dt);
                    //        foreach (var item in dt.AsEnumerable())
                    //        {
                    //            Console.WriteLine(item.ItemArray[0]);
                    //        }
                           
                    //    }
                  
                    //}
                }

            }
            _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            await Task.Delay(600000, stoppingToken);
        }
    }

}
