using EnterpriseKafka.Core;
using EnterpriseKafka.Web.Hubs;
using EnterpriseKafka.Web.Services;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllersWithViews();
builder.Services.AddSignalR();
builder.Services.AddEnterpriseKafka(options =>
{
    options.BootstrapServers = builder.Configuration.GetValue<string>("Kafka:BootstrapServers") ?? "localhost:9092";
    options.UseMiddleware<LoggingMiddleware>();
});
builder.Services.AddSingleton<KafkaTestConsoleService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<KafkaTestConsoleService>());

var app = builder.Build();
app.UseStaticFiles();
app.MapHub<KafkaMessagesHub>("/kafka-messages");
app.MapDefaultControllerRoute();
app.Run();
