using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace core_app
{
    public class Startup
    {
        public IConfiguration Configuration { get; }
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            //services.Add(new ServiceDescriptor(typeof(Helpers.DBHelper),new Helpers.DBHelper(Configuration.GetConnectionString("DefaultConnection"))));
            services.AddMvc();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            var log = loggerFactory.CreateLogger("rud-log:");
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            app.Use(async (context, next) =>
            {
                var timer = Stopwatch.StartNew();
                log.LogInformation("-------- Task Begin --------");
                await next();
                log.LogInformation($"time..{timer.ElapsedMilliseconds.ToString()}ms");
            });

            app.Map("/rud", action => action.Run(async context =>
            {
                Helpers.DBHelper dBHelper = new Helpers.DBHelper(Configuration.GetConnectionString("DefaultConnection"));
                dBHelper.CreateDBObjects(Helpers.DBHelper.DbProviders.MySql);


                var data = dBHelper.ExecuteReader("SELECT * FROM sakila.film;");
                string[] strArray = new string[data.FieldCount];
                for (int i = 0; i < data.FieldCount; i++)
                {
                    string str = data.GetName(i);//data["title"].ToString();
                    //await context.Response.WriteAsync($"title : {str}\n");
                    strArray[i] = str;
                }
                data.Close();
                var data2 = dBHelper.ExecuteReader("SELECT * FROM sakila.film;");
                while (data2.Read())
                {
                    foreach (var item in strArray)
                    {
                        string str2 = data2[item].ToString();
                        await context.Response.WriteAsync($"{item} : {str2}\n");
                    }
                }


                //var data2 = dBHelper.ExecuteScaler("select count(*) from sakila.film");
                //await context.Response.WriteAsync(data2.ToString());

            }));
            app.UseMvc();
            app.UseStaticFiles();
            app.Run(async (context) =>
            {
                await context.Response.WriteAsync("Hello World!");
            });
        }
    }
}
