using EnterpriseKafka.Core;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllersWithViews();
builder.Services.AddSignalR();
builder.Services.AddEnterpriseKafka();

var app = builder.Build();
app.UseStaticFiles();
app.MapDefaultControllerRoute();
app.MapHub<MetricsHub>("/hubs/metrics");
app.Run();

public class MetricsHub : Microsoft.AspNetCore.SignalR.Hub { }
