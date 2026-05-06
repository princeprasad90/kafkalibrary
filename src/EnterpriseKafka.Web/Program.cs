using EnterpriseKafka.Core;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllersWithViews();
builder.Services.AddEnterpriseKafka(options =>
{
    options.BootstrapServers = builder.Configuration.GetValue<string>("Kafka:BootstrapServers") ?? "localhost:9092";
    options.UseMiddleware<LoggingMiddleware>();
});

var app = builder.Build();
app.UseStaticFiles();
app.MapDefaultControllerRoute();
app.Run();
