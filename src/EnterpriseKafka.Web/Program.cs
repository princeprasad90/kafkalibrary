using EnterpriseKafka.Core;
using EnterpriseKafka.HealthChecks;
using EnterpriseKafka.Web;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllersWithViews();
builder.Services.AddSignalR();

builder.Services.AddEnterpriseKafka(opts =>
{
    opts.BootstrapServers = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
    opts.UseLogging();
});

builder.Services.AddHealthChecks()
    .AddKafkaBroker()
    .AddKafkaConsumerLag();

var app = builder.Build();

app.UseStaticFiles();
app.UseRouting();

app.MapDefaultControllerRoute();
app.MapHealthChecks("/health");
app.MapHub<MetricsHub>("/hubs/metrics");

app.Run();

